package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMesProductionProdlineGroup {
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
                "hdfs://10.0.0.14:8020/ck/ods_mes_production_prodline_group"
        );
        System.setProperty("HADOOP_USER_NAME", "jie");

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_group ( " +
                "CREATED_BY                  INT," +
                "CREATION_DATE               TIMESTAMP," +
                "LAST_UPDATED_BY             INT," +
                "LAST_UPDATE_DATE            TIMESTAMP," +
                "LAST_UPDATE_LOGIN           INT," +
                "SCHEDULE_REGION_ID          INT," +
                "PROD_LINE_GROUP_ID          INT," +
                "PROD_LINE_GROUP_CODE        STRING," +
                "DESCRIPTIONS                STRING," +
                "ORDER_BY_CODE               STRING," +
                "PLAN_START_TIME             TIMESTAMP," +
                "ENABLE_FLAG                 STRING," +
                "PROCESS_SEQUENCE            STRING," +
                "PERIODIC_TIME               STRING," +
                "BASIC_ALGORITHM             STRING," +
                "EXTENDED_ALGORITHM          STRING," +
                "FIX_TIME_FENCE              INT," +
                "FORWARD_PLANNING_TIME_FENCE INT," +
                "PROD_LINE_RULE              STRING," +
                "RELEASE_TIME_FENCE          INT," +
                "PLANNING_PHASE_TIME         STRING," +
                "PLANNING_BASE               STRING," +
                "DELAY_TIME_FENCE            INT," +
                "FROZEN_TIME_FENCE           INT," +
                "ORDER_TIME_FENCE            INT," +
                "RELEASE_CONCURRENT_RULE     STRING," +
                "PLAN_COLLABORATIVE_RULE     STRING," +
                "CID                         INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_PRODUCTION_LINE_GROUP', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_group (\n" +
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
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_group',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_group select " +
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
                "CURRENT_TIMESTAMP from ods_mes_production_prodline_group");

    }
}
