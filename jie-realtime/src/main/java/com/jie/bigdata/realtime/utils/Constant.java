package com.jie.bigdata.realtime.utils;

import org.apache.flink.api.common.time.Time;

public class Constant {
    //检查点的间隔
    public static long intervalCheckpoint = 600 * 1000L;

    //检查点的最小时间间隔
    public static long minPauseBetweenCheckpoints = 180 * 1000L;

    //检查点超时时间
    public static long checkpointTimeout = 600 * 1000L;

    //任务重试次数
    public static int failureRate = 4;

    //
    public static int restartAttempts = 2147483647;

    //Time interval for failures
    public static Time failureInterval = Time.days(1);

    //任务重启尝试延迟
    public static Time delayInterval = Time.minutes(5);

    //登录HDFS的用户
    public static String hadoopUserName = "root";

    //检查点存储地址
    public static String checkpointAddress = "hdfs://10.0.0.50:8020/ck/";

    //


}
