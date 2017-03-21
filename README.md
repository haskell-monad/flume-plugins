# flume-plugins

#多行匹配、断点续传
#默认会保存在本地redis的哈希FLUME_CONFIG中，key为CURRENT_LINE，value为读取到的行
#默认使用的db是4

#使用的源类
agent2.sources.a1.type = com.bigdata.flume.sources.multiline.MultiLineResumeExecSource
#行开始的标识
agent2.sources.a1.lineStartRegex = \\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}\\.\\d{3}
agent2.sources.a1.command=tail -F -n +0 /opt/AppService/logs/appService.log
agent2.sources.a1.channels=c1
agent2.sources.a1.restart=true
agent2.sources.a1.restartThrottle=5000
agent2.sources.a1.logStdErr=true
#redis配置
agent2.sources.a1.ip=localhost
agent2.sources.a1.port=6379
agent2.sources.a1.db=4
#拦截器配置
agent2.sources.a1.interceptors=i1 i2 i3
agent2.sources.a1.interceptors.i1.type=regex_filter
agent2.sources.a1.interceptors.i1.regex=.*\\s+ERROR\\s+
