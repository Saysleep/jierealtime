package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesSupplychainMaterialItem {
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
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //        failureRate, failureInterval, delayInterval
        //));
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage(
        //        checkpointAddress + "ods_mes_supplychain_material_item"
        //);
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql1 = "" +
                "CREATE TABLE ods_mes_supplychain_material_item1 ( " +
                    "CREATED_BY                  INT," +
                    "CREATION_DATE               TIMESTAMP," +
                    "LAST_UPDATED_BY             INT," +
                    "LAST_UPDATE_DATE            TIMESTAMP," +
                    "ITEM_ID                     INT," +
                    "PLANT_ID                    INT," +
                    "ITEM_CODE                   STRING," +
                    "PRIMARY_UOM                 STRING," +
                    "DESIGN_CODE                 STRING," +
                    "PLAN_CODE                   STRING," +
                    "ITEM_IDENTIFY_CODE          STRING," +
                    "ITEM_TYPE                   STRING," +
                    "MAKE_BUY_CODE               STRING," +
                    "SUPPLY_TYPE                 STRING," +
                    "KEY_COMPONENT_FLAG          STRING," +
                    "SCHEDULE_FLAG               STRING," +
                    "MAKE_TO_ORDER_FLAG          STRING," +
                    "PROD_LINE_RULE              STRING," +
                    "ITEM_SIZE                   STRING," +
                    "UNIT_WEIGHT                 INT," +
                    "ENABLE_FLAG                 STRING," +
                    "PRE_PROCESSING_LEAD_TIME    INT," +
                    "PROCESSING_LEAD_TIME        INT," +
                    "POST_PROCESSING_LEAD_TIME   INT," +
                    "SAFETY_LEAD_TIME            INT," +
                    "EXCEED_LEAD_TIME            INT," +
                    "DEMAND_TIME_FENCE           INT," +
                    "RELEASE_TIME_FENCE          INT," +
                    "ORDER_TIME_FENCE            INT," +
                    "DEMAND_MERGE_TIME_FENCE     STRING," +
                    "SUPPLY_MERGE_TIME_FENCE     STRING," +
                    "SAFETY_STOCK_METHOD         STRING," +
                    "SAFETY_STOCK_PERIOD         INT," +
                    "SAFETY_STOCK_VALUE          INT," +
                    "MAX_STOCK_QTY               INT," +
                    "MIN_STOCK_QTY               INT," +
                    "PRODUCT_CAPACITY_TIME_FENCE STRING," +
                    "PRODUCT_CAPACITY            INT," +
                    "ASSEMBLY_SHRINKAGE          INT," +
                    "ECONOMIC_LOT_SIZE           INT," +
                    "ECONOMIC_SPLIT_PARAMETER    INT," +
                    "MIN_ORDER_QTY               INT," +
                    "FIXED_LOT_MULTIPLE          INT," +
                    "PACK_QTY                    INT," +
                    "SEQUENCE_LOT_CONTROL        STRING," +
                    "ISSUE_CONTROL_TYPE          STRING," +
                    "ISSUE_CONTROL_QTY           INT," +
                    "COMPLETE_CONTROL_TYPE       STRING," +
                    "COMPLETE_CONTROL_QTY        INT," +
                    "EXPIRE_CONTROL_FLAG         STRING," +
                    "EXPIRE_DAYS                 INT," +
                    "RELEASE_CONCURRENT_RULE     STRING," +
                    "CID                         INT," +
                    "LAST_UPDATE_LOGIN           INT," +
                    "ITEM_GROUP_ID               INT," +
                    "RECOIL_SAFE_QTY             INT," +
                    "RECOIL_CALL_QTY             INT," +
                    "MO_SPLIT_FLAG               STRING," +
                    "RECOIL_FLAG                 STRING," +
                    "LOT_FLAG                    STRING," +
                    "REVIEW_FLAG                 STRING," +
                    "DES_LONG                    STRING," +
                    "SOURCE_ITEM_ID              INT," +
                    "ELECT_MACHINE_BRAND         STRING," +
                    "ELECT_MACHINE_V             STRING," +
                    "BRAKE_B_V                   STRING," +
                    "INDEP_FAN_V                 STRING," +
                    "PROTECT_LEVEL               STRING," +
                    "ELECT_MACHINE_FREQUENCY     STRING," +
                    "ELECT_MACHINE_REMARK1       STRING," +
                    "ELECT_MACHINE_REMARK2       STRING," +
                    "WEIGHT                      STRING," +
                    "`MODULE`                      STRING," +
                    "CASE_MODEL                  STRING," +
                    "ELECT_MACHINE_MODEL         STRING," +
                    "INSTALL                     STRING," +
                    "SPEED_RATIO                 STRING," +
                    "LENGTHS                     STRING," +
                    "WIDE                        STRING," +
                    "HIGH                        STRING," +
                    "ATTRIBUTE1                  STRING," +
                    "ATTRIBUTE2                  STRING," +
                    "ATTRIBUTE3                  STRING," +
                    "ATTRIBUTE4                  STRING," +
                    "ATTRIBUTE5                  STRING," +
                    "ATTRIBUTE6                  STRING," +
                    "ATTRIBUTE7                  STRING," +
                    "ATTRIBUTE8                  STRING," +
                    "ATTRIBUTE9                  STRING," +
                    "ATTRIBUTE10                 STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_ITEM_B', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        String sourceSql2 = "" +
                "CREATE TABLE ods_mes_supplychain_material_item2 ( " +
                    "CREATED_BY        INT," +
                    "CREATION_DATE     TIMESTAMP," +
                    "LAST_UPDATED_BY   INT," +
                    "LAST_UPDATE_DATE  TIMESTAMP," +
                    "LAST_UPDATE_LOGIN INT," +
                    "`LANGUAGE`         STRING," +
                    "SOURCE_LANGUAGE   STRING," +
                    "PLANT_ID          INT," +
                    "ITEM_ID           INT," +
                    "DESCRIPTIONS      STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_ITEM_TL', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        String sourceSql3 = "" +
                "CREATE TABLE ods_mes_supplychain_material_item3 ( " +
                "CREATED_BY         INT," +
                "CREATION_DATE      TIMESTAMP," +
                "LAST_UPDATED_BY    INT," +
                "LAST_UPDATE_DATE   TIMESTAMP," +
                "LAST_UPDATE_LOGIN  INT," +
                "SCHEDULE_REGION_ID INT," +
                "PLANT_ID           INT," +
                "PLANT_CODE         STRING," +
                "DESCRIPTIONS       STRING," +
                "ENABLE_FLAG        STRING," +
                "MAIN_PLANT_FLAG    STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_PLANT', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        String sourceSql4 = "" +
                "CREATE TABLE ods_mes_supplychain_material_item4 ( " +
                    "CREATED_BY        INT," +
                    "CREATION_DATE     TIMESTAMP," +
                    "LAST_UPDATED_BY   INT," +
                    "LAST_UPDATE_DATE  TIMESTAMP," +
                    "LAST_UPDATE_LOGIN INT," +
                    "ITEM_GROUP_ID     INT," +
                    "PLANT_ID          INT," +
                    "ITEM_GROUP_CODE   STRING," +
                    "DESCRIPTIONS      STRING," +
                    "SPLIT_NUM         INT," +
                    "ENABLE_FLAG       STRING," +
                    "CID               INT," +
                    "ATTRIBUTE1        STRING," +
                    "ATTRIBUTE2        STRING," +
                    "ATTRIBUTE3        STRING," +
                    "ATTRIBUTE4        STRING," +
                    "ATTRIBUTE5        STRING," +
                    "ATTRIBUTE6        STRING," +
                    "ATTRIBUTE7        STRING," +
                    "ATTRIBUTE8        STRING," +
                    "ATTRIBUTE9        STRING," +
                    "ATTRIBUTE10       STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_JIE_ITEM_GROUP', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql1);
        tableEnv.executeSql(sourceSql2);
        tableEnv.executeSql(sourceSql3);
        tableEnv.executeSql(sourceSql4);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_supplychain_material_item (\n" +
                    "MATERIAL_ID decimal," +
                    "CREATED_BY decimal," +
                    "CREATION_DATE timestamp," +
                    "LAST_UPDATED_BY decimal," +
                    "LAST_UPDATE_DATE timestamp," +
                    "PLANT_ID decimal," +
                    "ITEM_CODE string," +
                    "PRIMARY_UOM string," +
                    "DESIGN_CODE string," +
                    "PLAN_CODE string," +
                    "ITEM_IDENTIFY_CODE string," +
                    "ITEM_TYPE string," +
                    "MAKE_BUY_CODE string," +
                    "SUPPLY_TYPE string," +
                    "KEY_COMPONENT_FLAG string," +
                    "SCHEDULE_FLAG string," +
                    "MAKE_TO_ORDER_FLAG string," +
                    "PROD_LINE_RULE string," +
                    "ITEM_SIZE string," +
                    "UNIT_WEIGHT decimal," +
                    "ENABLE_FLAG string," +
                    "PRE_PROCESSING_LEAD_TIME decimal," +
                    "PROCESSING_LEAD_TIME decimal," +
                    "POST_PROCESSING_LEAD_TIME decimal," +
                    "SAFETY_LEAD_TIME decimal," +
                    "EXCEED_LEAD_TIME decimal," +
                    "DEMAND_TIME_FENCE decimal," +
                    "RELEASE_TIME_FENCE decimal," +
                    "ORDER_TIME_FENCE decimal," +
                    "DEMAND_MERGE_TIME_FENCE string," +
                    "SUPPLY_MERGE_TIME_FENCE string," +
                    "SAFETY_STOCK_METHOD string," +
                    "SAFETY_STOCK_PERIOD decimal," +
                    "SAFETY_STOCK_VALUE decimal," +
                    "MAX_STOCK_QTY decimal," +
                    "MIN_STOCK_QTY decimal," +
                    "PRODUCT_CAPACITY_TIME_FENCE string," +
                    "PRODUCT_CAPACITY decimal," +
                    "ASSEMBLY_SHRINKAGE decimal," +
                    "ECONOMIC_LOT_SIZE decimal," +
                    "ECONOMIC_SPLIT_PARAMETER decimal," +
                    "MIN_ORDER_QTY decimal," +
                    "FIXED_LOT_MULTIPLE decimal," +
                    "PACK_QTY decimal," +
                    "SEQUENCE_LOT_CONTROL string," +
                    "ISSUE_CONTROL_TYPE string," +
                    "ISSUE_CONTROL_QTY decimal," +
                    "COMPLETE_CONTROL_TYPE string," +
                    "COMPLETE_CONTROL_QTY decimal," +
                    "EXPIRE_CONTROL_FLAG string," +
                    "EXPIRE_DAYS decimal," +
                    "RELEASE_CONCURRENT_RULE string," +
                    "CID decimal," +
                    "DESCRIPTIONS string," +
                    "`LANGUAGE` string," +
                    "SCHEDULE_REGION_ID string," +
                    "ITEM_GROUP string," +
                    "`MODULE` string," +
                    "RECOIL_SAFE_QTY decimal," +
                    "RECOIL_CALL_QTY decimal," +
                    "MO_SPLIT_FLAG string," +
                    "RECOIL_FLAG string," +
                    "LOT_FLAG string," +
                    "DES_LONG string," +
                    "ELECT_MACHINE_BRAND string," +
                    "ELECT_MACHINE_V string," +
                    "BRAKE_B_V string," +
                    "INDEP_FAN_V string," +
                    "PROTECT_LEVEL string," +
                    "ELECT_MACHINE_FREQUENCY string," +
                    "ELECT_MACHINE_REMARK1 string," +
                    "ELECT_MACHINE_REMARK2 string," +
                    "WEIGHT string," +
                    "CASE_MODEL string," +
                    "ELECT_MACHINE_MODEL string," +
                    "INSTALL string," +
                    "SPEED_RATIO string," +
                    "LENGTHS string," +
                    "WIDE string," +
                    "HIGH string," +
                    "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_supplychain_material_item',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_supplychain_material_item select " +
                "B.ITEM_ID," +
                "B.CREATED_BY," +
                "B.CREATION_DATE," +
                "B.LAST_UPDATED_BY," +
                "B.LAST_UPDATE_DATE," +
                "B.PLANT_ID," +
                "B.ITEM_CODE," +
                "B.PRIMARY_UOM," +
                "B.DESIGN_CODE," +
                "B.PLAN_CODE," +
                "B.ITEM_IDENTIFY_CODE," +
                "B.ITEM_TYPE," +
                "B.MAKE_BUY_CODE," +
                "B.SUPPLY_TYPE," +
                "B.KEY_COMPONENT_FLAG," +
                "B.SCHEDULE_FLAG," +
                "B.MAKE_TO_ORDER_FLAG," +
                "B.PROD_LINE_RULE," +
                "B.ITEM_SIZE," +
                "B.UNIT_WEIGHT," +
                "B.ENABLE_FLAG," +
                "B.PRE_PROCESSING_LEAD_TIME," +
                "B.PROCESSING_LEAD_TIME," +
                "B.POST_PROCESSING_LEAD_TIME," +
                "B.SAFETY_LEAD_TIME," +
                "B.EXCEED_LEAD_TIME," +
                "B.DEMAND_TIME_FENCE," +
                "B.RELEASE_TIME_FENCE," +
                "B.ORDER_TIME_FENCE," +
                "B.DEMAND_MERGE_TIME_FENCE," +
                "B.SUPPLY_MERGE_TIME_FENCE," +
                "B.SAFETY_STOCK_METHOD," +
                "B.SAFETY_STOCK_PERIOD," +
                "B.SAFETY_STOCK_VALUE," +
                "B.MAX_STOCK_QTY," +
                "B.MIN_STOCK_QTY," +
                "B.PRODUCT_CAPACITY_TIME_FENCE," +
                "B.PRODUCT_CAPACITY," +
                "B.ASSEMBLY_SHRINKAGE," +
                "B.ECONOMIC_LOT_SIZE," +
                "B.ECONOMIC_SPLIT_PARAMETER," +
                "B.MIN_ORDER_QTY," +
                "B.FIXED_LOT_MULTIPLE," +
                "B.PACK_QTY," +
                "B.SEQUENCE_LOT_CONTROL," +
                "B.ISSUE_CONTROL_TYPE," +
                "B.ISSUE_CONTROL_QTY," +
                "B.COMPLETE_CONTROL_TYPE," +
                "B.COMPLETE_CONTROL_QTY," +
                "B.EXPIRE_CONTROL_FLAG," +
                "B.EXPIRE_DAYS," +
                "B.RELEASE_CONCURRENT_RULE," +
                "B.CID," +
                "T.DESCRIPTIONS," +
                "T.`LANGUAGE`," +
                "'1' SCHEDULE_REGION_ID," +
                "IG.ITEM_GROUP_CODE            ITEM_GROUP," +
                "B.`MODULE`," +
                "B.RECOIL_SAFE_QTY," +
                "B.RECOIL_CALL_QTY," +
                "B.MO_SPLIT_FLAG," +
                "B.RECOIL_FLAG," +
                "B.LOT_FLAG," +
                "B.DES_LONG," +
                "B.ELECT_MACHINE_BRAND," +
                "B.ELECT_MACHINE_V," +
                "B.BRAKE_B_V," +
                "B.INDEP_FAN_V," +
                "B.PROTECT_LEVEL," +
                "B.ELECT_MACHINE_FREQUENCY," +
                "B.ELECT_MACHINE_REMARK1," +
                "B.ELECT_MACHINE_REMARK2," +
                "B.WEIGHT," +
                "B.CASE_MODEL," +
                "B.ELECT_MACHINE_MODEL," +
                "B.INSTALL," +
                "B.SPEED_RATIO," +
                "B.LENGTHS," +
                "B.WIDE," +
                "B.HIGH," +
                "CURRENT_TIMESTAMP " +
                "FROM ods_mes_supplychain_material_item1 B," +
                "ods_mes_supplychain_material_item2 T," +
                "ods_mes_supplychain_material_item3 HP," +
                "ods_mes_supplychain_material_item4 IG " +
                "WHERE B.PLANT_ID = T.PLANT_ID " +
                "AND B.ITEM_ID = T.ITEM_ID " +
                "AND HP.PLANT_ID = B.PLANT_ID " +
                "AND T.`LANGUAGE` = 'ZHS' " +
                "AND B.ITEM_GROUP_ID = IG.ITEM_GROUP_ID");
    }
}
