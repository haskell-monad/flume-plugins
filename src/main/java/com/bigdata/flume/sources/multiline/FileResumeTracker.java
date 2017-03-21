package com.bigdata.flume.sources.multiline;

import com.bigdata.flume.utils.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 当前读取文件
 */
public class FileResumeTracker implements Serializable{

    private static final Logger logger = LoggerFactory.getLogger(FileResumeTracker.class);

    private RedisUtil redisUtil;
    private String currentLine;

    public FileResumeTracker(RedisUtil redisUtil) {
        this.redisUtil = redisUtil;
    }

    public String getCurrentLine() {
        return currentLine;
    }

    public void setCurrentLine(String currentLine) {
        this.currentLine = currentLine;
    }

    public String getHistoryLine(){
        return this.redisUtil.hget("FLUME_CONFIG","CURRENT_LINE");
    }

    public void saveCurrentLine(){
        this.redisUtil.hset("FLUME_CONFIG", "CURRENT_LINE",currentLine);
        logger.debug("保存当前行成功: {}",currentLine);
    }
}
