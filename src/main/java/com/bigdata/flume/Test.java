package com.bigdata.flume;

import com.bigdata.flume.sources.multiline.FileTracker;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Created by Administrator on 2017/1/5.
 */
public class Test {

    public static long customSort(File file){
        return file == null ? Long.MIN_VALUE : file.lastModified();
    }

    public static void findLastFile(){
        FileTracker fileTracker = new FileTracker();
        fileTracker.setHistoryFilePrefix("appService");
        fileTracker.setMonitorFileDir("C:\\Users\\Administrator\\Desktop\\demo\\history");
        File file = null;
        try {
            file = new File(fileTracker.getMonitorFileDir());
            Iterator<Path> iterator = Files.newDirectoryStream(
                    Paths.get(fileTracker.getMonitorFileDir()),
                    path -> path.toString().endsWith("log.zip") && path.toString().contains(fileTracker.getHistoryFilePrefix())
            ).iterator();
            ArrayList<Path> paths = Lists.newArrayList(iterator);
            File lastFile = paths.stream().map(path -> {
                return path.toFile();
            }).sorted(Comparator.comparing(f -> {return customSort(f);},(a, b) -> {
                    return Long.compare(b,a);
            })).findFirst().orElseGet(null);
            if(lastFile != null){
                System.out.println("zip文件个数：" + paths.size());
                System.out.println("最近的zip文件：" + lastFile.getName());
                System.out.println("最近的zip文件：" + lastFile.getPath());
            }
        }catch (IOException exception){
            exception.printStackTrace();
        }
    }

    public static void main(String args[]){
//        String command = "tail -F -n +0 /home/hadoop/alpha/flume/conf/appService.log";
//        String[] commandArgs = command.split("\\s+");
//        String monitorFile = "";
//        if(commandArgs != null && commandArgs.length > 0){
//            monitorFile = commandArgs[commandArgs.length - 1];
//        }
//        String monitorFileName = "";
//        String monitorFileDir = "";
//        if(StringUtils.isNotBlank(monitorFile)){
//            monitorFileName = monitorFile.substring(monitorFile.lastIndexOf("/") + 1,monitorFile.length());
//            monitorFileDir = monitorFile.substring(0,monitorFile.lastIndexOf("/"));
//        }
//        System.out.println(monitorFileName);
//        System.out.println(monitorFileDir);
//        findLastFile();

        System.out.println("2017-02-21 17:39:05.457".compareTo("2017-02-21 17:38:20.955") > 0 ? true : false);
    }
}
