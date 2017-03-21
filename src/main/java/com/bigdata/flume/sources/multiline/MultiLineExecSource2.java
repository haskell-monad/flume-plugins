package com.bigdata.flume.sources.multiline;

import com.bigdata.flume.utils.RedisUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 *
 */
public class MultiLineExecSource2 extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(MultiLineExecSource2.class);

    private String shell;
    private String command;
    private SourceCounter sourceCounter;
    private ExecutorService executor;
    private Future<?> runnerFuture;
    private long restartThrottle;
    private boolean restart;
    private boolean logStderr;
    private Integer bufferCount;
    private long batchTimeout;
    private ExecRunnable runner;
    private Charset charset;

    //新增断点续传、多行匹配
    private String regex;
    private String redisIp;
    private Integer redisPort;
    private Integer redisDB;
    private FileTracker fileTracker;
    private String monitorFile;//当前监控的文件
    private String historyFilePrefix;//历史文件前缀
    //新增断点续传

    @Override
    public void start() {
        logger.info("Exec source starting with command:{}", command);
        executor = Executors.newSingleThreadExecutor();
        runner = new ExecRunnable(shell, command, getChannelProcessor(), sourceCounter,
                restart, restartThrottle, logStderr, bufferCount, batchTimeout, charset, regex,fileTracker);
        // FIXME: Use a callback-like executor / future to signal us upon failure.
        runnerFuture = executor.submit(runner);
        sourceCounter.start();
        super.start();
        logger.debug("Exec source started");
    }

    @Override
    public void stop() {
        logger.info("Stopping exec source with command:{}", command);
        if(runner != null) {
            runner.setRestart(false);
            runner.kill();
        }
        if (runnerFuture != null) {
            logger.debug("Stopping exec runner");
            runnerFuture.cancel(true);
            logger.debug("Exec runner stopped");
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
            logger.debug("Waiting for exec executor service to stop");
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted while waiting for exec executor service to stop. Just exiting.");
                Thread.currentThread().interrupt();
            }
        }
        sourceCounter.stop();
        super.stop();
        logger.debug("保存追踪文件完成...");
        this.fileTracker.savePrevLine();
        this.fileTracker.saveCurrentLine();
        logger.debug("Exec source with command:{} stopped. Metrics:{}",command,sourceCounter);
    }

    @Override
    public void configure(Context context) {
        command = context.getString("command");
        Preconditions.checkState(command != null,"The parameter command must be specified");

        restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE, ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);
        restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART,ExecSourceConfigurationConstants.DEFAULT_RESTART);
        logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR, ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);
        bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE, ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
        batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT, ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);
        charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET, ExecSourceConfigurationConstants.DEFAULT_CHARSET));
        shell = context.getString(ExecSourceConfigurationConstants.CONFIG_SHELL, null);
        regex = context.getString(MultiLineExecSourceConfigurationConstants.REGEX, MultiLineExecSourceConfigurationConstants.DEFAULT_REGEX);

        //新增断点续传start
        monitorFile = context.getString(MultiLineExecSourceConfigurationConstants.MONITOR_FILE,"");
        Preconditions.checkState(monitorFile != null,"被监控的文件不可以为空,请设置");
        redisIp = context.getString(MultiLineExecSourceConfigurationConstants.REDIS_IP, MultiLineExecSourceConfigurationConstants.REDIS_DEFAULT_IP);
        redisPort = context.getInteger(MultiLineExecSourceConfigurationConstants.REDIS_PORT, MultiLineExecSourceConfigurationConstants.REDIS_DEFAULT_PORT);
        redisDB = context.getInteger(MultiLineExecSourceConfigurationConstants.REDIS_DB, MultiLineExecSourceConfigurationConstants.REDIS_DEFAULT_DB);
        String monitorFileName = "";
        String monitorFileDir = "";
        try {
            String[] commandArgs = command.split("\\s+");
            if(commandArgs != null && commandArgs.length > 0){
                monitorFile = commandArgs[commandArgs.length - 1];
            }
            if(StringUtils.isNotBlank(monitorFile)){
                monitorFileName = monitorFile.substring(monitorFile.lastIndexOf("/") + 1,monitorFile.length());
                monitorFileDir = monitorFile.substring(0,monitorFile.lastIndexOf("/"));
            }
            Preconditions.checkState(monitorFileName != null,"没有获取到监控的文件名");
            logger.debug("获取到被监控的文件: {}", monitorFileDir + "/" + monitorFileName);
            historyFilePrefix = context.getString(MultiLineExecSourceConfigurationConstants.HISTORY_FILE_PREFIX,monitorFileName);
            this.fileTracker = new FileTracker(monitorFileDir,monitorFileName,historyFilePrefix,new RedisUtil(redisIp,redisPort,redisDB));
        }catch (Exception e){
            e.printStackTrace();
        }
        //新增断点续传end
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    private static class ExecRunnable implements Runnable {
        private final String shell;
        private final String command;
        private final ChannelProcessor channelProcessor;
        private final SourceCounter sourceCounter;
        private volatile boolean restart;
        private final long restartThrottle;
        private final int bufferCount;
        private long batchTimeout;
        private final boolean logStderr;
        private final Charset charset;
        private Process process = null;
        private SystemClock systemClock = new SystemClock();
        private Long lastPushToChannel = systemClock.currentTimeMillis();
        private ScheduledExecutorService timedFlushService;
        private ScheduledFuture<?> future;
        //新增断点续传start
        private String regex;
        private Pattern pattern;
        private List<String> buffer = new ArrayList<>();
        private FileTracker fileTracker;
        private volatile boolean search;
        //新增断点续传end

        public ExecRunnable(String shell, String command, ChannelProcessor channelProcessor,
                            SourceCounter sourceCounter, boolean restart, long restartThrottle,
                            boolean logStderr, int bufferCount, long batchTimeout, Charset charset,
                            String regex,FileTracker fileTracker) {
            this.command = command;
            this.channelProcessor = channelProcessor;
            this.sourceCounter = sourceCounter;
            this.restartThrottle = restartThrottle;
            this.bufferCount = bufferCount;
            this.batchTimeout = batchTimeout;
            this.restart = restart;
            this.logStderr = logStderr;
            this.charset = charset;
            this.shell = shell;
            this.regex = regex;
            this.pattern = Pattern.compile(regex);
            this.fileTracker = fileTracker;
        }

        /**
         * 自定义排序
         * @param file
         * @return
         */
        public long customUpdateDateSort(File file){
            return file == null ? Long.MIN_VALUE : file.lastModified();
        }

        /**
         * 获取最近历史zip文件
         */
        public File findLastFile(){
            File lastFile = null;
            try {
                Iterator<Path> iterator = Files.newDirectoryStream(
                        Paths.get(fileTracker.getMonitorFileDir()),
                        path -> path.toString().endsWith("log.zip") && path.toString().contains(fileTracker.getHistoryFilePrefix())
                ).iterator();
                ArrayList<Path> paths = Lists.newArrayList(iterator);
                lastFile = paths.stream()
                        .map(path -> path.toFile())
                        .sorted(Comparator.comparing(f -> customUpdateDateSort(f), (a, b) -> Long.compare(b, a)))
                        .findFirst().orElseGet(null);
            }catch (Exception exception){
                logger.debug("获取最近历史zip文件失败",exception);
            }
            return lastFile;
        }

        /**
         * 过滤出来在指定修改时间范围内的文件列表
         * @param startTime
         * @param endTime
         * @return
         */
        public List<File> filterRangeFiles(long startTime,long endTime){
            List<File> files = null;
            try {
                Iterator<Path> iterator = Files.newDirectoryStream(
                        Paths.get(fileTracker.getMonitorFileDir()),
                        path -> path.toString().endsWith("log.zip") && path.toString().contains(fileTracker.getHistoryFilePrefix())
                ).iterator();
                ArrayList<Path> paths = Lists.newArrayList(iterator);
                files = paths.stream()
                        .map(path -> path.toFile())
                        .filter(file -> file.lastModified() >= startTime && file.lastModified() <= endTime)
                        .collect(Collectors.toList());
            }catch (Exception exception){
                logger.debug("获取最近历史zip文件失败",exception);
            }
            return files;
        }

        @Override
        public void run() {
            do {
                String exitCode = "unknown";
                BufferedReader reader = null;
                String line = null;
                final List<Event> eventList = new ArrayList<>();

                final String lastReadLine = fileTracker.getPrevLine();
                if(StringUtils.isBlank(lastReadLine)){
                    this.search = false;
                    logger.debug("没有历史记录，不需要进行断点续传...");
                    File lastFile = this.findLastFile();
                    if(lastFile != null){
                        fileTracker.saveNearHistoryFile(lastFile.getPath());
                        fileTracker.saveNearHistoryFileUpdate(lastFile.lastModified());
                        logger.debug("保存最近历史zip文件成功[path: {},lastModified: {}]", lastFile.getPath(),lastFile.lastModified());
                    }
                }else{
                    this.search = true;
                    logger.debug("该监控文件存在历史记录，上一次读取到: {}",lastReadLine);
                    File nearLastFile = this.findLastFile();
                    long lastUpdateDate = fileTracker.getNearHistoryFileUpdate();
                    if(nearLastFile == null){
                         logger.warn("没有获取到最近历史zip文件");
                    }else{
                        long nearLastUpdateDate = nearLastFile.lastModified();
                        if(lastUpdateDate == -1L){
                            fileTracker.saveNearHistoryFile(nearLastFile.getPath());
                            fileTracker.saveNearHistoryFileUpdate(nearLastFile.lastModified());
                            logger.debug("重新保存最近历史zip文件成功[path: {},lastModified: {}]", nearLastFile.getPath(),nearLastFile.lastModified());
                        }else if(nearLastUpdateDate == lastUpdateDate){
                            logger.debug("zip文件没有更新，不用收集");
                        }else{
                            //TODO 获取最近一次生成的zip文件?，和上次记录的zip文件A进行比较判断，
                            //TODO 判断?是否是最新的（最后修改日期），如果?是最新的,说明停止期间，日志发生了更多的滚动，
                            //TODO 需要处理[A-?]之间的zip文件
                            //TODO 记录zip文件最后读取到的行，和最后一次处理的zip文件
                            logger.debug("zip文件有更新，需要收集新增加的zip文件列表");
                            List<File> files = this.filterRangeFiles(lastUpdateDate, nearLastUpdateDate);
                            String zipFileName = fileTracker.getZipFileName();
                            String zipCurrentLine = fileTracker.getZipCurrentLine();
                            for (File file : files){
                                try {
                                    fileTracker.setZipFileName(file.getName());
                                    fileTracker.setZipFileLastDate(file.lastModified());
                                    reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                                    boolean flag = false;
                                    if(zipFileName.equals(file.getName())){
                                        flag = true;
                                    }
                                    while ((line = reader.readLine()) != null) {
                                        synchronized (eventList) {
                                            Matcher m = pattern.matcher(line);
                                            if(m.find()) {
                                                int gc = m.groupCount();
                                                logger.debug("新行正则匹配到[{}]个,第1个: {}", gc, m.group(0));
                                                if(flag){
                                                    if(!m.group(0).equals(zipCurrentLine)){
                                                        buffer.clear();
                                                        break;
                                                    }else{
                                                        flag = false;
                                                    }
                                                }
                                                //开始处理行
                                                if(buffer.size() != 0) {
                                                    sourceCounter.incrementEventReceivedCount();
                                                    StringBuffer sbuffer = new StringBuffer();
                                                    for(int i = 0; i < buffer.size(); ++i) {
                                                        sbuffer.append(buffer.get(i)+"\n");
                                                    }
                                                    eventList.add(EventBuilder.withBody(sbuffer.toString().getBytes(charset)));
                                                    if(eventList.size() >= bufferCount || timeout()) {
                                                        flushEventBatch(eventList);
                                                    }
                                                    buffer.clear();
                                                }
                                                buffer.add(line);
                                                fileTracker.setZipCurrentLine(m.group(0));
                                            }else {
                                                buffer.add(line);
                                            }
                                        }
                                    }
                                    synchronized (eventList) {
                                        if(!buffer.isEmpty()) {
                                            sourceCounter.incrementEventReceivedCount();
                                            //multiline setting start
                                            StringBuffer sbuffer = new StringBuffer();
                                            for(int i = 0; i < buffer.size(); ++i) {
                                                sbuffer.append(buffer.get(i)+"\n");
                                            }
                                            buffer.clear();
                                            eventList.add(EventBuilder.withBody(sbuffer.toString().getBytes(charset)));
                                            //multiline setting end
                                        }
                                        if(!eventList.isEmpty()) {
                                            flushEventBatch(eventList);
                                        }
                                    }
                                    logger.debug("zip文件[{}]处理完毕",file.getName());
                                }catch (Exception e){
                                    e.printStackTrace();
                                    logger.error("zip文件[{}]处理失败",file.getPath());
                                }
                            }
                        }
                    }
                }
                try {
                    if(shell != null) {
                        String[] commandArgs = formulateShellCommand(shell, command);
                        process = Runtime.getRuntime().exec(commandArgs);
                    }else {
                        String[] commandArgs = command.split("\\s+");
                        process = new ProcessBuilder(commandArgs).start();
                    }
                    reader = new BufferedReader(new InputStreamReader(process.getInputStream(), charset));
                    StderrReader stderrReader = new StderrReader(new BufferedReader(new InputStreamReader(process.getErrorStream(), charset)), logStderr);
                    stderrReader.setName("StderrReader-[" + command + "]");
                    stderrReader.setDaemon(true);
                    stderrReader.start();
                    timedFlushService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("timedFlushExecService" + Thread.currentThread().getId() + "-%d").build());
                    future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
                          @Override
                          public void run() {
                              try {
                                  synchronized (eventList) {
                                      if(!search && !eventList.isEmpty() && timeout()) {
                                          flushEventBatch(eventList);
                                      }
                                  }
                              } catch (Exception e) {
                                  logger.error("处理该批次事件出现异常", e);
                                  if(e instanceof InterruptedException) {
                                      Thread.currentThread().interrupt();
                                  }
                              }
                          }
                    },batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);
                    boolean seek = false;
                    while ((line = reader.readLine()) != null) {
                        seek = true;
                        synchronized (eventList) {
                            //多行匹配start
                            Matcher m = pattern.matcher(line);
                            if(m.find()) {
                                int gc = m.groupCount();
                                logger.debug("新行正则匹配到[{}]个,第1个: {}", gc, m.group(0));
                                if(this.search){
                                    buffer.clear();
                                    if(lastReadLine.equals(m.group(0))){
                                        this.search = false;
                                    }else if(!lastReadLine.equals(m.group(0))){
                                        continue;
                                    }
                                }
                                //开始处理行
                                if(buffer.size() != 0) {
                                    sourceCounter.incrementEventReceivedCount();
                                    StringBuffer sbuffer = new StringBuffer();
                                    for(int i = 0; i < buffer.size(); ++i) {
                                        sbuffer.append(buffer.get(i)+"\n");
                                    }
                                    eventList.add(EventBuilder.withBody(sbuffer.toString().getBytes(charset)));
                                    if(eventList.size() >= bufferCount || timeout()) {
                                        flushEventBatch(eventList);
                                    }
                                    buffer.clear();
                                }
                                buffer.add(line);
                                //记录读取的行信息
                                fileTracker.setPrevLine(m.group(0));
                                fileTracker.setCurrentLine(m.group(0));
                            }else {
                                buffer.add(line);
                            }
                            //多行匹配end
                        }
                    }
                    if(seek && search && line == null){
                        //说明在当前文件中没有查询到日志，需要去历史压缩包中查询
                        //收集历史压缩包中的日志,记录历史压缩包文件名:读取到的行:最后读取到的行:是否读完
                        //TODO
                    }
                    synchronized (eventList) {
                        if(!this.search && !buffer.isEmpty()) {
                            sourceCounter.incrementEventReceivedCount();
                            //multiline setting start
                            StringBuffer sbuffer = new StringBuffer();
                            for(int i = 0; i < buffer.size(); ++i) {
                                sbuffer.append(buffer.get(i)+"\n");
                            }
                            buffer.clear();
                            eventList.add(EventBuilder.withBody(sbuffer.toString().getBytes(charset)));
                            //multiline setting end
                        }
                        if(!eventList.isEmpty()) {
                            flushEventBatch(eventList);
                        }
                    }
                } catch (Exception e) {
                    logger.error("运行命令失败: {}",command, e);
                    if(e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException ex) {
                            logger.error("关闭exec source reader失败", ex);
                        }
                    }
                    exitCode = String.valueOf(kill());
                }
                if(restart) {
                    logger.info("重启 in {}ms, exit code {}", restartThrottle,exitCode);
                    try {
                        Thread.sleep(restartThrottle);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.info("命令 [" + command + "] 退出码 " + exitCode);
                }
            } while(restart);
        }

        private void flushEventBatch(List<Event> eventList){
            channelProcessor.processEventBatch(eventList);
            sourceCounter.addToEventAcceptedCount(eventList.size());
            eventList.clear();
            lastPushToChannel = systemClock.currentTimeMillis();
        }

        private boolean timeout(){
            return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
        }

        private static String[] formulateShellCommand(String shell, String command) {
            String[] shellArgs = shell.split("\\s+");
            String[] result = new String[shellArgs.length + 1];
            System.arraycopy(shellArgs, 0, result, 0, shellArgs.length);
            result[shellArgs.length] = command;
            return result;
        }

        public int kill() {
            if(process != null) {
                synchronized (process) {
                    //保存文件start
                    this.search = true;
                    this.fileTracker.saveCurrentLine();
                    this.fileTracker.savePrevLine();
                    //保存文件end
                    process.destroy();
                    try {
                        int exitValue = process.waitFor();
                        // Stop the Thread that flushes periodically
                        if (future != null) {
                            future.cancel(true);
                        }
                        if (timedFlushService != null) {
                            timedFlushService.shutdown();
                            while (!timedFlushService.isTerminated()) {
                                try {
                                    timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    logger.debug("Interrupted while waiting for exec executor service to stop. Just exiting.");
                                    Thread.currentThread().interrupt();
                                }
                            }
                        }
                        return exitValue;
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Integer.MIN_VALUE;
            }
            return Integer.MIN_VALUE / 2;
        }
        public void setRestart(boolean restart) {
            this.restart = restart;
        }
    }

    private static class StderrReader extends Thread {
        private BufferedReader input;
        private boolean logStderr;

        protected StderrReader(BufferedReader input, boolean logStderr) {
            this.input = input;
            this.logStderr = logStderr;
        }

        @Override
        public void run() {
            try {
                int i = 0;
                String line = null;
                while((line = input.readLine()) != null) {
                    if(logStderr) {
                        // There is no need to read 'line' with a charset
                        // as we do not to propagate it.
                        // It is in UTF-16 and would be printed in UTF-8 format.
                        logger.info("StderrLogger[{}] = '{}'", ++i, line);
                    }
                }
            } catch (IOException e) {
                logger.info("StderrLogger exiting", e);
            } finally {
                try {
                    if(input != null) {
                        input.close();
                    }
                } catch (IOException ex) {
                    logger.error("Failed to close stderr reader for exec source", ex);
                }
            }
        }
    }

    /**
     * 读取zip包文件
     * @param file
     * @throws Exception
     */
    public static void readZipFile(String file) throws Exception {
        ZipFile zf = new ZipFile(file);
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        ZipInputStream zin = new ZipInputStream(in);
        ZipEntry ze;
        while ((ze = zin.getNextEntry()) != null) {
            if (ze.isDirectory()) {
            } else {
                System.err.println("file - " + ze.getName() + " : "
                        + ze.getSize() + " bytes");
                long size = ze.getSize();
                if (size > 0) {
                    BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(ze)));
                    String line;
                    while ((line = br.readLine()) != null) {
                        System.out.println(line);
                    }
                    br.close();
                }
                System.out.println();
            }
        }
        zin.closeEntry();
    }
}
