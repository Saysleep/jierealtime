package com.jie.bigdata.realtime.utils;

import org.apache.flink.api.common.time.Time;

public class Constant {

    //检查点的间隔
    public static long intervalCheckpoint = 180 * 1000L;

    //检查点与检查点的最小停顿
    public static long minPauseBetweenCheckpoints = 30 * 1000L;

    //检查点超时时间
    public static long checkpointTimeout = 600 * 1000L;

    //在作业被声明为失败之前重试执行的次数（设置为极大值，使得总任务不会一个小任务检查点失败而重启）
    public static int restartAttempts = 2147483647;

    //两次连续重新启动尝试之间的延迟
    public static Time delayInterval = Time.minutes(1);

    //登录HDFS的用户
    public static String hadoopUserName = "root";

    //检查点存储地址
    public static String checkpointAddress = "hdfs://10.0.0.14:8020/ck/";

    //配置流服务器驱动
    public static String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    //配置流服务器名称
    public static final String MYSQL_SERVER = "jdbc:mysql://10.0.0.51:3306";


}
