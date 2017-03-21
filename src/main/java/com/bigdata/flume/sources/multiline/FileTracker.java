package com.bigdata.flume.sources.multiline;

import com.bigdata.flume.utils.RedisUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Created by Administrator on 2017/1/3.
 */
public class FileTracker implements Serializable{

    private static final Logger logger = LoggerFactory.getLogger(FileTracker.class);

    private String monitorFileName; //当前监控的文件名
    private String monitorFileDir; //当前监控的文件所在目录
    private String prevLine;//上次读取到的行.日期（已读完）
    private String currentLine;//当前读取到的行。（未读完）
    private long lastUpdateDate;//文件最后修改日期
    private long lastFileSize; //文件最后大小
    private String nextZipFileName;//下一个滚动产生的文件名
    private RedisUtil redisUtil;
    private String historyFilePrefix;//历史压缩文件前缀

    private static final String MAP_KEY = "tracker_";

    private String zipFileName;
    private long zipFileLastDate;
    private String zipCurrentLine;



    public FileTracker() {
    }

    public FileTracker(String monitorFileDir,String monitorFileName,String historyFilePrefix,RedisUtil redisUtil) {
        this.monitorFileDir = monitorFileDir;
        this.monitorFileName = monitorFileName;
        this.historyFilePrefix = historyFilePrefix;
        this.redisUtil = redisUtil;
        this.redisUtil.hset(MAP_KEY+monitorFileName,"HISTORY_FILE_PREFIX",historyFilePrefix);
    }

    public String getZipCurrentLine() {
        return zipCurrentLine;
    }

    public void setZipCurrentLine(String zipCurrentLine) {
        this.zipCurrentLine = zipCurrentLine;
    }

    public long getLastUpdateDate() {
        return lastUpdateDate;
    }

    public void setLastUpdateDate(long lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }

    public long getZipFileLastDate() {
        return zipFileLastDate;
    }

    public void setZipFileLastDate(long zipFileLastDate) {
        this.zipFileLastDate = zipFileLastDate;
    }

    public String getZipFileName() {
        return zipFileName;
    }

    public void setZipFileName(String zipFileName) {
        this.zipFileName = zipFileName;
    }

    /**
     * 获取上一次读取到的行
     * @return
     */
    public String getPrevLine() {
        this.prevLine = this.redisUtil.hget(MAP_KEY+this.getMonitorFileName(),"PREV_LINE");
        return prevLine;
    }

    public void setPrevLine(String prevLine) {
        this.prevLine = prevLine;
    }

    public String getCurrentLine() {
        return currentLine;
    }

    public void setCurrentLine(String currentLine) {
        this.currentLine = currentLine;
    }

    public long getLastFileSize() {
        return lastFileSize;
    }

    public void setLastFileSize(long lastFileSize) {
        this.lastFileSize = lastFileSize;
    }

    public String getNextZipFileName() {
        return nextZipFileName;
    }

    public void setNextZipFileName(String nextZipFileName) {
        this.nextZipFileName = nextZipFileName;
    }

    public String getMonitorFileName() {
        return monitorFileName;
    }

    public String getMonitorFileDir() {
        return monitorFileDir;
    }

    /**
     * 保存被监控文件名称
     * @param monitorFileName
     */
    public void setMonitorFileName(String monitorFileName) {
        this.monitorFileName = monitorFileName;
        this.redisUtil.hset(MAP_KEY+monitorFileName,"MONITOR_FILE_NAME",monitorFileName);
    }

    public String getHistoryFilePrefix() {
        return historyFilePrefix;
    }

    public void setHistoryFilePrefix(String historyFilePrefix) {
        this.historyFilePrefix = historyFilePrefix;
    }

    /**
     * 保存被监控文件所在目录
     * @param monitorFileDir
     */
    public void setMonitorFileDir(String monitorFileDir) {
        this.monitorFileDir = monitorFileDir;
//        this.redisUtil.hset(MAP_KEY+monitorFileName,"MONITOR_FILE_DIR",monitorFileDir);
    }

    /**
     * 保存当前行
     */
    public void saveCurrentLine(){
        this.redisUtil.hset(MAP_KEY+monitorFileName,"CURRENT_LINE",currentLine);
    }

    /**
     * 保存上一行
     */
    public void savePrevLine(){
        //使用读取到的当前行替换上一次保存的行
        this.redisUtil.hset(MAP_KEY+monitorFileName,"PREV_LINE",currentLine);
    }

    /**
     * 保存历史zip文件名
     * @param historyFileName
     */
    public void saveNearHistoryFile(String historyFileName){
        this.redisUtil.hset(MAP_KEY+monitorFileName,"HISTORY_FILE_NAME",historyFileName);
    }

    /**
     * 保存历史zip文件最后修改时间
     * @param lastUpdateDate
     */
    public void saveNearHistoryFileUpdate(Long lastUpdateDate){
        this.redisUtil.hset(MAP_KEY+monitorFileName,"HISTORY_FILE_DATE",lastUpdateDate.toString());
    }

    /**
     * 获取历史zip文件最后修改时间
     */
    public long getNearHistoryFileUpdate(){
        String result = this.redisUtil.hget(MAP_KEY + monitorFileName, "HISTORY_FILE_DATE");
        return StringUtils.isBlank(result) ? -1L : Long.parseLong(result);
    }

    /**
     * 保存读取的历史zip文件名
     */
    public void saveReadHistoryName(){
        this.redisUtil.hset(MAP_KEY+monitorFileName,"ZIP_FILE_NAME",zipFileName);
    }

    /**
     * 保存读取的历史zip文件修改时间
     */
    public void saveReadHistoryUpdate(){
        this.redisUtil.hset(MAP_KEY+monitorFileName,"ZIP_FILE_DATE",Long.toString(zipFileLastDate));
    }

    /**
     * 保存读取的历史zip文件行
     */
    public void saveReadHistoryLine(){
        this.redisUtil.hset(MAP_KEY+monitorFileName,"HISTORY_FILE_DATE",this.getZipCurrentLine());
    }

    /**
     * 保存读取的历史zip文件名
     */
    public String getReadHistoryName(){
        return this.redisUtil.hget(MAP_KEY + monitorFileName, "ZIP_FILE_NAME");
    }

    /**
     * 保存读取的历史zip文件修改时间
     */
    public long getReadHistoryUpdate(){
        String result = this.redisUtil.hget(MAP_KEY + monitorFileName, "ZIP_FILE_DATE");
        return StringUtils.isBlank(result) ? -1L : Long.parseLong(result);
    }

    /**
     * 保存读取的历史zip文件行
     */
    public String getReadHistoryLine(){
        return this.redisUtil.hget(MAP_KEY + monitorFileName, "HISTORY_FILE_DATE");
    }

}
