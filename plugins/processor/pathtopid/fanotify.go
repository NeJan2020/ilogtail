package pathtopid

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/s3rj1k/go-fanotify/fanotify"
	"golang.org/x/sys/unix"
)

const MAX_MARK = 8192

type fanotifyCache struct {
	notify   *fanotify.NotifyFD
	hostDir  string
	maxFiles int

	// path2pid -> host_dir + realPath -> info
	path2pid map[string]*info
	mu       sync.RWMutex

	// parse Mount
	rawPath2MountPath map[string]string
	parseMount        bool

	context pipeline.Context
}

type eventType int

const (
	CLOSE eventType = iota
	MODIFY
)

type notifyEvent struct {
	path   string
	pid    int
	evType eventType
}

func (f *fanotifyCache) Init(context pipeline.Context) {
	f.context = context
	notify, err := fanotify.Initialize(
		unix.FAN_CLOEXEC|
			unix.FAN_CLASS_NOTIF,
		os.O_RDONLY|
			unix.O_LARGEFILE|
			unix.O_CLOEXEC,
	)
	if err != nil {
		logger.Errorf(context.GetRuntimeContext(), "INIT NOTIFY FAILED", "init notify failed, err: %v", err)
	} else {
		logger.Info(context.GetRuntimeContext(), "init notify success")
		f.notify = notify
	}
	if val, ok := os.LookupEnv("HOST_DIR"); ok {
		f.hostDir = val
	} else {
		f.hostDir = ""
	}
	f.maxFiles = 0
	f.path2pid = make(map[string]*info)
}

func (f *fanotifyCache) AddPath(path string) {
	if f.maxFiles >= MAX_MARK {
		logger.Warning(f.context.GetRuntimeContext(), "add path err: maxfiles reached")
		return
	}

	logger.Info(f.context.GetRuntimeContext(), "message", "AddPath", "path", path)
	if err := f.notify.Mark(
		unix.FAN_MARK_ADD,
		unix.FAN_MODIFY|
			unix.FAN_CLOSE_WRITE,
		unix.AT_FDCWD,
		path,
	); err != nil {
		logger.Errorf(f.context.GetRuntimeContext(), "add mark notify", "path: %v, err: %v", path, err)
		return
	}
	f.maxFiles++
}

func (f *fanotifyCache) RemovePath(path string) {
	if f.maxFiles < 1 {
		logger.Warning(context.Background(), "remove path err: not watch file")
		return
	}

	if err := f.notify.Mark(
		unix.FAN_MARK_REMOVE,
		0,
		unix.AT_FDCWD,
		path,
	); err != nil {
		logger.Errorf(context.Background(), "remove mark notify", "path: %v, err: %v", path, err)
		return
	}
	f.maxFiles--
}

func (f *fanotifyCache) continueGetEvent(eventCh chan<- *notifyEvent) {
	for {
		ev, err := f.getEvent()
		if err == nil && ev != nil {
			eventCh <- ev
		}
		if err != nil {
			logger.Errorf(context.Background(), "get event", "err: %v", err)
		}
	}
}

func (f *fanotifyCache) startWatchLifeCycle() {
	ch := make(chan *notifyEvent, 100)
	go f.continueGetEvent(ch)

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case event := <-ch:
			f.handleEvent(event)
		case <-ticker.C:
			f.cleanExpired()
		}
	}
}

func (f *fanotifyCache) handleEvent(event *notifyEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_info, ok := f.path2pid[event.path]
	if !ok {
		_info = &info{
			timestamp: time.Now().UnixNano(),
			pid:       event.pid,
			init:      true,
		}
		f.path2pid[event.path] = _info
		logger.Info(f.context.GetRuntimeContext(), "path2PID", "created", "path", event.path, "pid", event.pid)
		return
	}

	if event.evType == CLOSE {
		return
	}

	logger.Info(f.context.GetRuntimeContext(), "path2PID", "updated", "path", event.path, "pid", event.pid)
	_info.timestamp = time.Now().UnixNano()
	_info.pid = event.pid
	_info.init = true
}

func (f *fanotifyCache) getEvent() (*notifyEvent, error) {
	var event notifyEvent
	data, err := f.notify.GetEvent(os.Getpid())

	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}

	if data == nil {
		return nil, nil
	}

	defer data.Close()

	var ev_type eventType

	if data.MatchMask(unix.FAN_CLOSE_WRITE) {
		ev_type = CLOSE
	} else if data.MatchMask(unix.FAN_MODIFY) {
		ev_type = MODIFY
	} else {
		return nil, nil
	}

	path, err := data.GetPath()
	// 从事件中取出的事件的path不包含hostDir
	// 可能是容器内路径

	if f.parseMount {
		if len(f.rawPath2MountPath) > 1e3 {
			f.rawPath2MountPath = make(map[string]string)
		}

		key := fmt.Sprintf("%d@@%s", data.Pid, path)
		if mountPath, ok := f.rawPath2MountPath[key]; ok {
			path = mountPath
		} else {
			path = f.parseMountInfo(data, path)
			f.rawPath2MountPath[key] = path
		}
	}

	path = f.hostDir + path

	if ev_type == MODIFY {
		f.notify.Mark(unix.FAN_MARK_ADD|unix.FAN_MARK_IGNORED_MASK|unix.FAN_MARK_IGNORED_SURV_MODIFY, unix.FAN_MODIFY, unix.AT_FDCWD, path)
	} else if ev_type == CLOSE {
		f.notify.Mark(unix.FAN_MARK_REMOVE|unix.FAN_MARK_IGNORED_MASK|unix.FAN_MARK_IGNORED_SURV_MODIFY, unix.FAN_MODIFY, unix.AT_FDCWD, path)
	}

	if err != nil {
		return nil, err
	}
	event.path = path
	event.pid = data.GetPID()
	event.evType = ev_type

	return &event, nil
}

func (f *fanotifyCache) parseMountInfo(data *fanotify.EventMetadata, rawPath string) string {
	fd, err := data.GetFdInfo()
	if err != nil {
		return rawPath
	}

	mountID := []byte(strconv.Itoa(fd.MountID))
	pid := data.GetPID()

	file, err := os.Open(fmt.Sprintf("/proc/%d/mountinfo", pid))
	if err != nil {
		return rawPath
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		mountPoint, source, ok := parseMountInfoLine(line, mountID)
		if !ok {
			continue
		}

		if strings.HasPrefix(rawPath, string(mountPoint)) {
			// 只替换一次，避免 mountPoint 在路径中间出现时误替换
			return string(source) + rawPath[len(mountPoint):]
		}
		break
	}

	if err := scanner.Err(); err != nil {
		logger.Error(f.context.GetRuntimeContext(), "PATH2PID read mountinfo failed", "err", err)
	}
	
	return rawPath
}

// parseMountInfoLine 解析 mountinfo 的一行，快速匹配 mountID。
// 返回 mountPoint, source, 是否匹配。
func parseMountInfoLine(line, targetID []byte) (mountPoint, source []byte, ok bool) {
	// 1. 找第一个空格，拿到 mountID
	i := bytes.IndexByte(line, ' ')
	if i == -1 || !bytes.Equal(line[:i], targetID) {
		return nil, nil, false
	}

	// 2. 跳过 parentID, major:minor, root，取到 mountPoint
	fieldStart := i + 1
	fieldCount := 0
	var mountIdxStart, mountIdxEnd int
	for j := fieldStart; j < len(line); j++ {
		if line[j] == ' ' {
			fieldCount++
			if fieldCount == 3 {
				mountIdxStart = j + 1
			} else if fieldCount == 4 {
				mountIdxEnd = j
				break
			}
		}
	}
	if mountIdxStart == 0 || mountIdxEnd == 0 {
		return nil, nil, false
	}
	mountPoint = line[mountIdxStart:mountIdxEnd]

	// 3. 找 "-"，后面第一个字段是 fstype，第二个字段是 source
	if dash := bytes.IndexByte(line, '-'); dash != -1 {
		srcStart := dash + 1
		// 跳过空格
		for srcStart < len(line) && line[srcStart] == ' ' {
			srcStart++
		}
		// 跳过 fstype
		for srcStart < len(line) && line[srcStart] != ' ' {
			srcStart++
		}
		for srcStart < len(line) && line[srcStart] == ' ' {
			srcStart++
		}
		// source
		srcEnd := srcStart
		for srcEnd < len(line) && line[srcEnd] != ' ' {
			srcEnd++
		}
		if srcStart < srcEnd {
			source = line[srcStart:srcEnd]
		}
	}

	return mountPoint, source, true
}

func (f *fanotifyCache) cleanExpired() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, v := range f.path2pid {
		if time.Now().UnixNano()-v.timestamp > time.Hour.Nanoseconds() {
			f.RemovePath(k)
			delete(f.path2pid, k)
		}
	}
}

func (f *fanotifyCache) getPidFromPath(path string) *info {
	f.mu.RLock()
	defer f.mu.RUnlock()
	info, ok := f.path2pid[path]
	if ok {
		return info
	}
	return nil
}

func (f *fanotifyCache) addPathWatch(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.AddPath(path)
	info := &info{
		pid:       0,
		timestamp: time.Now().UnixNano(),
		init:      false,
	}
	f.path2pid[path] = info
}
