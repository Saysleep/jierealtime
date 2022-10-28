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

public class OdsMesProductionProdlineMakeOrder {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        //env.enableCheckpointing(intervalCheckpoint, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        //env.getCheckpointConfig().enableExternalizedCheckpoints(
        //        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        //);
        ////env.setRestartStrategy(RestartStrategies.failureRateRestart(
        ////        failureRate, failureInterval, delayInterval
        ////));
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts,delayInterval));
        //env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage(
        //        checkpointAddress + "ods_mes_production_prodline_make_order"
        //);
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_make_order ( " + 
                    "CREATED_BY                INT," +
                    "CREATION_DATE             TIMESTAMP," +
                    "LAST_UPDATED_BY           INT," +
                    "LAST_UPDATE_DATE          TIMESTAMP," +
                    "LAST_UPDATE_LOGIN         INT," +
                    "CID                       INT," +
                    "MAKE_ORDER_ID             INT," +
                    "MAKE_ORDER_NUM            STRING," +
                    "PLANT_ID                  INT," +
                    "SCHEDULE_REGION_ID        INT," +
                    "ITEM_ID                   INT," +
                    "MO_QTY                    INT," +
                    "SUPPLY_QTY                INT," +
                    "PLAN_QTY                  INT," +
                    "DEMAND_DATE               TIMESTAMP," +
                    "MAKE_ORDER_TYPE           STRING," +
                    "PRODUCTION_VERSION        STRING," +
                    "PRIORITY                  INT," +
                    "MAKE_ORDER_STATUS         STRING," +
                    "MO_LAST_STATUS            STRING," +
                    "ERP_JOB_TYPE_ID           INT," +
                    "DEMAND_ORDER_ID           INT," +
                    "USER_DEMAND_DATE          TIMESTAMP," +
                    "PROD_LINE_ID              INT," +
                    "PROD_LINE_FIX_FLAG        STRING," +
                    "SCHEDULE_RELEASE_TIME     TIMESTAMP," +
                    "EARLIEST_START_TIME       TIMESTAMP," +
                    "START_TIME                TIMESTAMP," +
                    "FPS_TIME                  TIMESTAMP," +
                    "FPC_TIME                  TIMESTAMP," +
                    "LPS_TIME                  TIMESTAMP," +
                    "LPC_TIME                  TIMESTAMP," +
                    "FULFILL_TIME              TIMESTAMP," +
                    "TOP_MO_ID                 INT," +
                    "PARENT_MO_ID              INT," +
                    "SOURCE_MO_ID              INT," +
                    "REFERENCE_MO_ID           INT," +
                    "MO_REFERENCE_TYPE         STRING," +
                    "MO_CAPACITY               INT," +
                    "RATE                      INT," +
                    "RATE_TYPE                 STRING," +
                    "DIE_PLANNING_ID           INT," +
                    "PLAN_TYPE                 STRING," +
                    "PLANNING_FLAG             STRING," +
                    "EXPLORED_FLAG             STRING," +
                    "DERIVED_FLAG              STRING," +
                    "MO_WARNNING_FLAG          STRING," +
                    "ENDING_PROCESS_FLAG       STRING," +
                    "BOM_LEVEL                 STRING," +
                    "EXCEED_LEAD_TIME          INT," +
                    "PRE_PROCESSING_LEAD_TIME  INT," +
                    "PROCESSING_LEAD_TIME      INT," +
                    "POST_PROCESSING_LEAD_TIME INT," +
                    "SAFETY_LEAD_TIME          INT," +
                    "SWITCH_TIME               INT," +
                    "SWITCH_TIME_USED          INT," +
                    "RELEASE_TIME_FENCE        INT," +
                    "ORDER_TIME_FENCE          INT," +
                    "RELEASED_DATE             TIMESTAMP," +
                    "RELEASED_BY               INT," +
                    "CLOSED_DATE               TIMESTAMP," +
                    "CLOSED_BY                 INT," +
                    "SPECIAL_COLOR             STRING," +
                    "PLANNING_REMARK           STRING," +
                    "REMARK                    STRING," +
                    "ATTRIBUTE1                STRING," +
                    "ATTRIBUTE2                STRING," +
                    "ATTRIBUTE3                STRING," +
                    "ATTRIBUTE4                STRING," +
                    "ATTRIBUTE5                STRING," +
                    "ATTRIBUTE6                STRING," +
                    "ATTRIBUTE7                STRING," +
                    "ATTRIBUTE8                STRING," +
                    "ATTRIBUTE9                STRING," +
                    "ATTRIBUTE10               STRING," +
                    "ATTRIBUTE11               STRING," +
                    "ATTRIBUTE12               STRING," +
                    "ATTRIBUTE13               STRING," +
                    "ATTRIBUTE14               STRING," +
                    "ATTRIBUTE15               STRING," +
                    "SO_NO                     STRING," +
                    "SO_ITEM                   STRING," +
                    "ENABLE_FLAG               STRING," +
                    "HEAT_SPLIT_FLAG           STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HPS_MAKE_ORDER', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_make_order (\n" +
                "MAKE_ORDER_ID decimal," +
                "CREATED_BY decimal," +
                "CREATION_DATE timestamp," +
                "LAST_UPDATED_BY decimal," +
                "LAST_UPDATE_DATE timestamp," +
                "LAST_UPDATE_LOGIN decimal," +
                "CID decimal," +
                "manufacture_order_id string," +
                "PLANT_ID decimal," +
                "SCHEDULE_REGION_ID decimal," +
                "material_id decimal," +
                "MO_QTY decimal," +
                "SUPPLY_QTY decimal," +
                "PLAN_QTY decimal," +
                "DEMAND_DATE timestamp," +
                "MAKE_ORDER_TYPE string," +
                "PRODUCTION_VERSION string," +
                "PRIORITY decimal," +
                "MAKE_ORDER_STATUS string," +
                "MO_LAST_STATUS string," +
                "ERP_JOB_TYPE_ID decimal," +
                "DEMAND_ORDER_ID decimal," +
                "USER_DEMAND_DATE timestamp," +
                "PROD_LINE_ID decimal," +
                "PROD_LINE_FIX_FLAG string," +
                "SCHEDULE_RELEASE_TIME timestamp," +
                "EARLIEST_START_TIME timestamp," +
                "START_TIME timestamp," +
                "FPS_TIME timestamp," +
                "FPC_TIME timestamp," +
                "LPS_TIME timestamp," +
                "LPC_TIME timestamp," +
                "FULFILL_TIME timestamp," +
                "TOP_MO_ID decimal," +
                "PARENT_MO_ID decimal," +
                "SOURCE_MO_ID decimal," +
                "REFERENCE_MO_ID decimal," +
                "MO_REFERENCE_TYPE string," +
                "MO_CAPACITY decimal," +
                "RATE decimal," +
                "RATE_TYPE string," +
                "DIE_PLANNING_ID decimal," +
                "PLAN_TYPE string," +
                "PLANNING_FLAG string," +
                "EXPLORED_FLAG string," +
                "DERIVED_FLAG string," +
                "MO_WARNNING_FLAG string," +
                "ENDING_PROCESS_FLAG string," +
                "BOM_LEVEL string," +
                "EXCEED_LEAD_TIME decimal," +
                "PRE_PROCESSING_LEAD_TIME decimal," +
                "PROCESSING_LEAD_TIME decimal," +
                "POST_PROCESSING_LEAD_TIME decimal," +
                "SAFETY_LEAD_TIME decimal," +
                "SWITCH_TIME decimal," +
                "SWITCH_TIME_USED decimal," +
                "RELEASE_TIME_FENCE decimal," +
                "ORDER_TIME_FENCE decimal," +
                "RELEASED_DATE timestamp," +
                "RELEASED_BY decimal," +
                "CLOSED_DATE timestamp," +
                "CLOSED_BY decimal," +
                "SPECIAL_COLOR string," +
                "PLANNING_REMARK string," +
                "REMARK string," +
                "ATTRIBUTE1 string," +
                "ATTRIBUTE2 string," +
                "ATTRIBUTE3 string," +
                "ATTRIBUTE4 string," +
                "ATTRIBUTE5 string," +
                "ATTRIBUTE6 string," +
                "ATTRIBUTE7 string," +
                "ATTRIBUTE8 string," +
                "ATTRIBUTE9 string," +
                "ATTRIBUTE10 string," +
                "ATTRIBUTE11 string," +
                "ATTRIBUTE12 string," +
                "ATTRIBUTE13 string," +
                "ATTRIBUTE14 string," +
                "ATTRIBUTE15 string," +
                "sale_order_id string," +
                "sale_row_id string," +
                "ENABLE_FLAG string," +
                "HEAT_SPLIT_FLAG string," +
                "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_make_order',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_make_order select " +
                "  MAKE_ORDER_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", CID" +
                ", CONCAT('000',MAKE_ORDER_NUM) as MAKE_ORDER_NUM" +
                ", PLANT_ID" +
                ", SCHEDULE_REGION_ID" +
                ", ITEM_ID" +
                ", MO_QTY" +
                ", SUPPLY_QTY" +
                ", PLAN_QTY" +
                ", DEMAND_DATE" +
                ", MAKE_ORDER_TYPE" +
                ", PRODUCTION_VERSION" +
                ", PRIORITY" +
                ", MAKE_ORDER_STATUS" +
                ", MO_LAST_STATUS" +
                ", ERP_JOB_TYPE_ID" +
                ", DEMAND_ORDER_ID" +
                ", USER_DEMAND_DATE" +
                ", PROD_LINE_ID" +
                ", PROD_LINE_FIX_FLAG" +
                ", SCHEDULE_RELEASE_TIME" +
                ", EARLIEST_START_TIME" +
                ", START_TIME" +
                ", FPS_TIME" +
                ", FPC_TIME" +
                ", LPS_TIME" +
                ", LPC_TIME" +
                ", FULFILL_TIME" +
                ", TOP_MO_ID" +
                ", PARENT_MO_ID" +
                ", SOURCE_MO_ID" +
                ", REFERENCE_MO_ID" +
                ", MO_REFERENCE_TYPE" +
                ", MO_CAPACITY" +
                ", RATE" +
                ", RATE_TYPE" +
                ", DIE_PLANNING_ID" +
                ", PLAN_TYPE" +
                ", PLANNING_FLAG" +
                ", EXPLORED_FLAG" +
                ", DERIVED_FLAG" +
                ", MO_WARNNING_FLAG" +
                ", ENDING_PROCESS_FLAG" +
                ", BOM_LEVEL" +
                ", EXCEED_LEAD_TIME" +
                ", PRE_PROCESSING_LEAD_TIME" +
                ", PROCESSING_LEAD_TIME" +
                ", POST_PROCESSING_LEAD_TIME" +
                ", SAFETY_LEAD_TIME" +
                ", SWITCH_TIME" +
                ", SWITCH_TIME_USED" +
                ", RELEASE_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", RELEASED_DATE" +
                ", RELEASED_BY" +
                ", CLOSED_DATE" +
                ", CLOSED_BY" +
                ", SPECIAL_COLOR" +
                ", PLANNING_REMARK" +
                ", REMARK" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", ATTRIBUTE6" +
                ", ATTRIBUTE7" +
                ", ATTRIBUTE8" +
                ", ATTRIBUTE9" +
                ", ATTRIBUTE10" +
                ", ATTRIBUTE11" +
                ", ATTRIBUTE12" +
                ", ATTRIBUTE13" +
                ", ATTRIBUTE14" +
                ", ATTRIBUTE15" +
                ", SO_NO" +
                ", SO_ITEM" +
                ", ENABLE_FLAG" +
                ", HEAT_SPLIT_FLAG" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_make_order");
    }
}

