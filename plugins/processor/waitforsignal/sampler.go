package signalsampler

import (
	"strconv"
	"sync"

	"github.com/alibaba/ilogtail/pkg/logger"
	"github.com/alibaba/ilogtail/pkg/models"
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
)

const pluginName = "processor_wait_for_signal"
const MAX_CACHE_MEM_BYTE = 300 * 1024 * 1024
const MAX_CACHE_ITEM_SIZE = 3e5

// 注册插件
func init() {
	pipeline.Processors[pluginName] = func() pipeline.Processor {
		return &SignalSampler{}
	}
}

/*
SignalSampler 基于信号对输入的数据进行采样
*/
type SignalSampler struct {
	// 关闭信号采样 用于测试和全量日志采集
	DisableSignalSampler bool
	SamplerId            int
	// 最大存储内存
	MaxCacheByte uint64

	// Move To tags
	ContentsRename map[string]string
	// 日志计数器
	logCounter uint
	logChan    chan<- *CacheLog

	// 下次要导出的日志
	logEventExposed   []*protocol.Log
	logEventExposedV2 []*models.PipelineGroupEvents

	// 接收信号时要准备下次导出数据和每次处理输入后，带走导出数据冲突
	exposedMutex sync.RWMutex
	// 初始化失败的标记，如果初始化失败，不在做存储
	initWithError bool
	context       pipeline.Context
}

func (s *SignalSampler) Init(context pipeline.Context) error {
	s.context = context
	if !s.DisableSignalSampler {
		s.logChan = RegisterSampler(s)
	}
	logger.Info(context.GetRuntimeContext(), "init signal sampler with id", s.SamplerId)
	return nil
}

func (*SignalSampler) Description() string {
	return "cache and wait for signal"
}

func (s *SignalSampler) Process(in *models.PipelineGroupEvents, context pipeline.PipelineContext) {
	var containerID, pidStr string
	if in != nil && in.Group != nil && in.Group.Tags != nil {
		newTags := make(map[string]string)
		for k, v := range in.Group.Tags.Iterator() {
			if rename, find := s.ContentsRename[k]; find {
				newTags[rename] = v
			}

			if k == "_container_id_" {
				containerID = v
			}
			if k == "pid" {
				pidStr = v
			}
		}
		for k, v := range newTags {
			in.Group.Tags.Add(k, v)
		}
	}

	if s.initWithError || s.DisableSignalSampler {
		s.logCounter++
		if in.Group != nil && in.Group.Tags != nil {
			in.Group.Tags.Add("_time_ns_", "0")
		}
		for i := 0; i < len(in.Events); i++ {
			in.Events[i].GetTags().Add("log_seq", strconv.FormatUint(uint64(s.logCounter)+uint64(i), 10))
		}
		s.logCounter += uint(len(in.Events) - 1)
		context.Collector().Collect(in.Group, in.Events...)
		return
	}

	if len(containerID) > 0 || len(pidStr) > 0 {
		var earliest uint64 = 0
		for _, event := range in.Events {
			eTS := event.GetTimestamp()
			if eTS < earliest || earliest == 0 {
				earliest = eTS
			}
		}

		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			pid = 0
		}
		s.logCounter++
		s.cacheLogV2(in, containerID, pid, earliest)
		s.logCounter += uint(len(in.Events) - 1) // skip log_seq for grouped event
	}

	exposed := s.GetExposedLogV2()
	for i := 0; i < len(exposed); i++ {
		context.Collector().Collect(exposed[i].Group, exposed[i].Events...)
	}
}

func (s *SignalSampler) cacheLogV2(in *models.PipelineGroupEvents, containerId string, pid int, logTime uint64) {
	cachedLog := CacheLog{
		LogRef: LogRef{
			v2Log: in,
		},
		SourceKeyRef: SourceKey{
			FromSampler: s.SamplerId,
			ContainerId: containerId,
			ProcessPid:  pid,
		},
		LogCapturedTime: logTime,
		GlobalIndex: GlobalIndex{
			// ProcessIndex: 0,
			LogIndexInProcess: s.logCounter,
		},
	}

	s.logChan <- &cachedLog
}

func (s *SignalSampler) addExposedV2Log(outLog *CacheLog) {
	if outLog.v2Log == nil {
		return
	}

	v2Log := outLog.GetV2Log()

	v2Log.Group.Tags.Add("_time_ns_", "0") // can not sort v2Event by time_ns
	for i := 0; i < len(v2Log.Events); i++ {
		v2Log.Events[i].GetTags().Add("log_seq", strconv.FormatUint(uint64(s.logCounter)+uint64(i), 10))
	}

	s.logEventExposedV2 = append(s.logEventExposedV2, v2Log)
}

func (s *SignalSampler) AddExposedLog(outLog *CacheLog) {
	s.exposedMutex.Lock()
	defer s.exposedMutex.Unlock()
	if outLog.v1Log != nil {
		s.addExposedV1Log(outLog)
	}
	s.addExposedV2Log(outLog)
}

func (s *SignalSampler) AddExposedLogs(outLogs []*CacheLog) {
	s.exposedMutex.Lock()
	defer s.exposedMutex.Unlock()
	for _, outLog := range outLogs {
		if outLog.v1Log != nil {
			s.addExposedV1Log(outLog)
		} else {
			s.addExposedV2Log(outLog)
		}
	}
}

func (s *SignalSampler) GetExposedLogV2() []*models.PipelineGroupEvents {
	if len(s.logEventExposedV2) == 0 {
		return nil
	}

	s.exposedMutex.Lock()
	defer s.exposedMutex.Unlock()
	out := s.logEventExposedV2
	s.logEventExposedV2 = nil
	return out
}
