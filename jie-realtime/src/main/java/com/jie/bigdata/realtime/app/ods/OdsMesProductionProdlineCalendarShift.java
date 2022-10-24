package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMesProductionProdlineCalendarShift {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://10.0.0.50:8020/ck/ods_mes_production_prodline_calendar_shift"
        );
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_calendar_shift ( " +
                "CREATED_BY         INT," +
                "CREATION_DATE      TIMESTAMP," +
                "LAST_UPDATED_BY    INT," +
                "LAST_UPDATE_DATE   TIMESTAMP," +
                "LAST_UPDATE_LOGIN  INT," +
                "CALENDAR_SHIFT_ID  INT," +
                "CALENDAR_ID        INT," +
                "CALENDAR_DAY       TIMESTAMP," +
                "SHIFT_CODE         STRING," +
                "ENABLE_FLAG        STRING," +
                "SHIFT_START_TIME   TIMESTAMP," +
                "SHIFT_END_TIME     TIMESTAMP," +
                "BREAK_TIME         INT," +
                "ACTIVITY           INT," +
                "REPLENISH_CAPACITY INT," +
                "AVAILABLE_TIME     INT," +
                "AVAILABLE_CAPACITY INT," +
                "REMARK             STRING," +
                "CID                INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_CALENDAR_SHIFT', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_calendar_shift (\n" +
                    "calendar_shift_id decimal(27,3)," +
                    "created_by int," +
                    "creation_date timestamp," +
                    "last_updated_by int," +
                    "last_update_date timestamp," +
                    "last_update_login int," +
                    "calendar_id int," +
                    "calendar_day timestamp," +
                    "shift_code string," +
                    "enable_flag string," +
                    "shift_start_time timestamp," +
                    "shift_end_time timestamp," +
                    "break_time int," +
                    "activity int," +
                    "replenish_capacity int," +
                    "available_time int," +
                    "available_capacity int," +
                    "remark string," +
                    "cid int," +
                    "update_datetime timestamp" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_calendar_shift',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_calendar_shift select" +
                " CALENDAR_SHIFT_ID\n" +
                ",CREATED_BY\n" +
                ",CREATION_DATE\n" +
                ",LAST_UPDATED_BY\n" +
                ",LAST_UPDATE_DATE\n" +
                ",LAST_UPDATE_LOGIN\n" +
                ",CALENDAR_ID\n" +
                ",CALENDAR_DAY\n" +
                ",SHIFT_CODE\n" +
                ",ENABLE_FLAG\n" +
                ",SHIFT_START_TIME\n" +
                ",SHIFT_END_TIME\n" +
                ",BREAK_TIME\n" +
                ",ACTIVITY\n" +
                ",REPLENISH_CAPACITY\n" +
                ",AVAILABLE_TIME\n" +
                ",AVAILABLE_CAPACITY\n" +
                ",REMARK\n" +
                ",CID" +
                ",CURRENT_TIMESTAMP from ods_mes_production_prodline_calendar_shift");
    }
}
