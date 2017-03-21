package com.bigdata.flume.sources.multiline;

import com.bigdata.flume.utils.RedisUtil;
import com.google.common.base.Preconditions;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 多行匹配，断点续传，不支持读取历史zip包
 */
public class MultiLineResumeExecSource extends AbstractSource implements EventDrivenSource, Configurable {

    private static final Logger logger = LoggerFactory.getLogger(MultiLineResumeExecSource.class);

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
    private FileResumeTracker fileTracker;
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
        logger.info("执行stop操作,save currentLine: {},historyLine: {}", this.fileTracker.getCurrentLine(), this.fileTracker.getHistoryLine());
        this.fileTracker.saveCurrentLine();
        logger.debug("Exec source with command:{} stopped. Metrics:{}",command,sourceCounter);
    }

    @Override
    public void configure(Context context) {
        command = context.getString("command");
        Preconditions.checkState(command != null,"The parameter command must be specified");

        restartThrottle = context.getLong(ExecSourceConfigurationConstants.CONFIG_RESTART_THROTTLE, ExecSourceConfigurationConstants.DEFAULT_RESTART_THROTTLE);
        restart = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_RESTART, ExecSourceConfigurationConstants.DEFAULT_RESTART);
        logStderr = context.getBoolean(ExecSourceConfigurationConstants.CONFIG_LOG_STDERR, ExecSourceConfigurationConstants.DEFAULT_LOG_STDERR);
        bufferCount = context.getInteger(ExecSourceConfigurationConstants.CONFIG_BATCH_SIZE, ExecSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
        batchTimeout = context.getLong(ExecSourceConfigurationConstants.CONFIG_BATCH_TIME_OUT, ExecSourceConfigurationConstants.DEFAULT_BATCH_TIME_OUT);
        charset = Charset.forName(context.getString(ExecSourceConfigurationConstants.CHARSET, ExecSourceConfigurationConstants.DEFAULT_CHARSET));
        shell = context.getString(ExecSourceConfigurationConstants.CONFIG_SHELL, null);
        regex = context.getString(MultiLineExecSourceConfigurationConstants.REGEX, MultiLineExecSourceConfigurationConstants.DEFAULT_REGEX);

        //新增断点续传start
        redisIp = context.getString(MultiLineExecSourceConfigurationConstants.REDIS_IP, MultiLineExecSourceConfigurationConstants.REDIS_DEFAULT_IP);
        redisPort = context.getInteger(MultiLineExecSourceConfigurationConstants.REDIS_PORT, MultiLineExecSourceConfigurationConstants.REDIS_DEFAULT_PORT);
        redisDB = context.getInteger(MultiLineExecSourceConfigurationConstants.REDIS_DB, MultiLineExecSourceConfigurationConstants.REDIS_DEFAULT_DB);
        try {
            this.fileTracker = new FileResumeTracker(new RedisUtil(redisIp,redisPort,redisDB));
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
        private FileResumeTracker fileTracker;
        //新增断点续传end

        public ExecRunnable(String shell, String command, ChannelProcessor channelProcessor,
                            SourceCounter sourceCounter, boolean restart, long restartThrottle,
                            boolean logStderr, int bufferCount, long batchTimeout, Charset charset,
                            String regex,FileResumeTracker fileTracker) {
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
            this.pattern = Pattern.compile(this.regex);
            this.fileTracker = fileTracker;
        }

        @Override
        public void run() {
            do {
                String exitCode = "unknown";
                BufferedReader reader = null;
                String line = null;
                final List<Event> eventList = new ArrayList<>();
                final String lastReadLine = fileTracker.getHistoryLine();
                if(!StringUtils.isBlank(lastReadLine)){
                    logger.info("该监控文件存在历史记录，上一次读取到: {}", lastReadLine);
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
                                      if(!eventList.isEmpty() && timeout()) {
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
                    while ((line = reader.readLine()) != null) {
                        synchronized (eventList) {
                            //多行匹配start
                            Matcher m = pattern.matcher(line);
                            if(m.find()) {
                                logger.info("新行开始[history: {},current: {}]",lastReadLine,m.group(0));
                                if(StringUtils.isNotBlank(lastReadLine) && lastReadLine.compareTo(m.group(0)) > 0){
                                    //如果历史记录行大于当前行的话，则跳过本行
                                    logger.info("跳过当前行[history: {},current: {}]",lastReadLine,m.group(0));
                                    continue;
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
                                fileTracker.setCurrentLine(m.group(0));
                            }else {
                                buffer.add(line);
                            }
                            //多行匹配end
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
            logger.info("执行flushEventBatch操作,save currentLine: {},historyLine: {}", this.fileTracker.getCurrentLine(), this.fileTracker.getHistoryLine());
            this.fileTracker.saveCurrentLine();
            fileTracker.saveCurrentLine();
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
                    logger.info("执行kill操作,save currentLine: {},historyLine: {}", this.fileTracker.getCurrentLine(),this.fileTracker.getHistoryLine());
                    this.fileTracker.saveCurrentLine();
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
}
