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

public class OdsMesProductionProdlineLineItem {
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
                checkpointAddress + "ods_mes_production_prodline_line_item"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);
        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_line_item ( " + 
                    "CREATED_BY                INT," +
                    "CREATION_DATE             TIMESTAMP," +
                    "LAST_UPDATED_BY           INT," +
                    "LAST_UPDATE_DATE          TIMESTAMP," +
                    "LAST_UPDATE_LOGIN         INT," +
                    "RELATION_ID               INT," +
                    "PLANT_ID                  INT," +
                    "ITEM_ID                   INT," +
                    "PROD_LINE_ID              INT," +
                    "PRODUCTION_VERSION        STRING," +
                    "RATE_TYPE                 STRING," +
                    "RATE                      INT," +
                    "ACTIVITY                  INT," +
                    "PRIORITY                  INT," +
                    "PROCESS_BATCH             INT," +
                    "STANDARD_RATE_TYPE        STRING," +
                    "STANDARD_RATE             INT," +
                    "AUTO_ASSIGN_FLAG          STRING," +
                    "PRE_PROCESSING_LEAD_TIME  INT," +
                    "PROCESSING_LEAD_TIME      INT," +
                    "POST_PROCESSING_LEAD_TIME INT," +
                    "SAFETY_LEAD_TIME          INT," +
                    "ISSUE_WAREHOUSE_CODE      STRING," +
                    "ISSUE_LOCATOR_CODE        STRING," +
                    "COMPLETE_WAREHOUSE_CODE   STRING," +
                    "COMPLETE_LOCATOR_CODE     STRING," +
                    "INVENTORY_WAREHOUSE_CODE  STRING," +
                    "INVENTORY_LOCATOR_CODE    STRING," +
                    "CID                       INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_PROD_LINE_ITEM', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_line_item (\n" + 
                    "prod_line_id decimal," +
                    "created_by decimal," +
                    "creation_date timestamp," +
                    "last_updated_by decimal," +
                    "last_update_date timestamp," +
                    "last_update_login decimal," +
                    "relation_id decimal," +
                    "plant_id decimal," +
                    "material_id decimal," +
                    "production_version string," +
                    "rate_type string," +
                    "rate decimal," +
                    "activity decimal," +
                    "priority decimal," +
                    "process_batch decimal," +
                    "standard_rate_type string," +
                    "standard_rate decimal," +
                    "auto_assign_flag string," +
                    "pre_processing_lead_time decimal," +
                    "processing_lead_time decimal," +
                    "post_processing_lead_time decimal," +
                    "safety_lead_time decimal," +
                    "issue_warehouse_code string," +
                    "issue_locator_code string," +
                    "complete_warehouse_code string," +
                    "complete_locator_code string," +
                    "inventory_warehouse_code string," +
                    "inventory_locator_code string," +
                    "cid decimal," +
                    "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_line_item',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_line_item select " +
                "  PROD_LINE_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", RELATION_ID" +
                ", PLANT_ID" +
                ", ITEM_ID" +
                ", PRODUCTION_VERSION" +
                ", RATE_TYPE" +
                ", RATE" +
                ", ACTIVITY" +
                ", PRIORITY" +
                ", PROCESS_BATCH" +
                ", STANDARD_RATE_TYPE" +
                ", STANDARD_RATE" +
                ", AUTO_ASSIGN_FLAG" +
                ", PRE_PROCESSING_LEAD_TIME" +
                ", PROCESSING_LEAD_TIME" +
                ", POST_PROCESSING_LEAD_TIME" +
                ", SAFETY_LEAD_TIME" +
                ", ISSUE_WAREHOUSE_CODE" +
                ", ISSUE_LOCATOR_CODE" +
                ", COMPLETE_WAREHOUSE_CODE" +
                ", COMPLETE_LOCATOR_CODE" +
                ", INVENTORY_WAREHOUSE_CODE" +
                ", INVENTORY_LOCATOR_CODE" +
                ", CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_line_item");

    }
}
