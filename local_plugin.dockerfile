FROM registry.cn-hangzhou.aliyuncs.com/kindlingx/loongcollector-alpha:apo-3.1.4-f9453a3

COPY output/libGoPluginBase.so libGoPluginBase.so

CMD ["/usr/local/loongcollector/loongcollector_control.sh", "start_and_block"]