package com.bigdata.flume.sources.multiline;


/**
 * 变量定义
 */
public class MultiLineExecSourceConfigurationConstants {

    /**
     * 定义行开始标识
     */
	public static final String REGEX = "lineStartRegex";
	public static final String DEFAULT_REGEX = "\\s?\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d";

    /**
     * 记录所在文件
     */
    public static final String REDIS_IP = "ip";
    public static final String REDIS_PORT = "port";
    public static final String REDIS_DB = "db";
    public static final String REDIS_DEFAULT_IP = "localhost";
    public static final Integer REDIS_DEFAULT_PORT = 6379;
    public static final Integer REDIS_DEFAULT_DB = 4;


    /**
     * 被监控的文件
     * tail -F -n +0 /home/hadoop/alpha/flume/conf/appService.log
     */
    public static final String MONITOR_FILE = "monitorFile";

    /**
     * 历史文件前缀
     * 默认为被监控的文件名 appService
     */
    public static final String HISTORY_FILE_PREFIX = "historyFilePrefix";
}
