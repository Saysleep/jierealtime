package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;
import static com.jie.bigdata.realtime.utils.Constant.hadoopUserName;

public class OdsMesProductionProdlineCalendar {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(intervalCheckpoint, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                failureRate, failureInterval, delayInterval
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                checkpointAddress + "ods_mes_production_prodline_calendar"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_calendar ( " +
                "CREATED_BY INT ," +
                "CREATION_DATE TIMESTAMP ," +
                "LAST_UPDATED_BY INT ," +
                "LAST_UPDATE_DATE TIMESTAMP ," +
                "LAST_UPDATE_LOGIN INT ," +
                "CALENDAR_ID INT ," +
                "CALENDAR_TYPE STRING ," +
                "DESCRIPTION STRING ," +
                "PROD_LINE_ID INT ," +
                "ENABLE_FLAG STRING ," +
                "PLANT_ID INT ," +
                "CALENDAR_CODE STRING ," +
                "CID INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_CALENDAR', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_calendar (\n" +
                "calendar_id decimal(27,3)," +
                "created_by double," +
                "creation_date TIMESTAMP," +
                "last_updated_by double," +
                "last_update_date TIMESTAMP," +
                "last_update_login double," +
                "calendar_type string," +
                "description string," +
                "prod_line_id double," +
                "enable_flag string," +
                "plant_id double," +
                "calendar_code string," +
                "cid double," +
                "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_calendar',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_calendar select " +
                "CALENDAR_ID,\n" +
                "CREATED_BY,\n" +
                "CREATION_DATE,\n" +
                "LAST_UPDATED_BY,\n" +
                "LAST_UPDATE_DATE,\n" +
                "LAST_UPDATE_LOGIN,\n" +
                "CALENDAR_TYPE,\n" +
                "DESCRIPTION,\n" +
                "PROD_LINE_ID,\n" +
                "ENABLE_FLAG,\n" +
                "PLANT_ID,\n" +
                "CALENDAR_CODE,\n" +
                "CID," +
                "CURRENT_TIMESTAMP from ods_mes_production_prodline_calendar");

    }
}
