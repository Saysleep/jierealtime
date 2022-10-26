package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesSupplychainMaterialItemB {
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
                checkpointAddress + "ods_mes_supplychain_material_item_b"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_supplychain_material_item_b ( " +
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
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_supplychain_material_item_b (\n" +
                    "material_id decimal," +
                    "created_by decimal," +
                    "creation_date timestamp," +
                    "last_updated_by decimal," +
                    "last_update_date timestamp," +
                    "plant_id decimal," +
                    "material_id_2 string," +
                    "primary_uom string," +
                    "design_code string," +
                    "plan_code string," +
                    "item_identify_code string," +
                    "item_type string," +
                    "make_buy_code string," +
                    "supply_type string," +
                    "key_component_flag string," +
                    "schedule_flag string," +
                    "make_to_order_flag string," +
                    "prod_line_rule string," +
                    "item_size string," +
                    "unit_weight decimal," +
                    "enable_flag string," +
                    "pre_processing_lead_time decimal," +
                    "processing_lead_time decimal," +
                    "post_processing_lead_time decimal," +
                    "safety_lead_time decimal," +
                    "exceed_lead_time decimal," +
                    "demand_time_fence decimal," +
                    "release_time_fence decimal," +
                    "order_time_fence decimal," +
                    "demand_merge_time_fence string," +
                    "supply_merge_time_fence string," +
                    "safety_stock_method string," +
                    "safety_stock_period decimal," +
                    "safety_stock_value decimal," +
                    "max_stock_qty decimal," +
                    "min_stock_qty decimal," +
                    "product_capacity_time_fence string," +
                    "product_capacity decimal," +
                    "assembly_shrinkage decimal," +
                    "economic_lot_size decimal," +
                    "economic_split_parameter decimal," +
                    "min_order_qty decimal," +
                    "fixed_lot_multiple decimal," +
                    "pack_qty decimal," +
                    "sequence_lot_control string," +
                    "issue_control_type string," +
                    "issue_control_qty decimal," +
                    "complete_control_type string," +
                    "complete_control_qty decimal," +
                    "expire_control_flag string," +
                    "expire_days decimal," +
                    "release_concurrent_rule string," +
                    "cid decimal," +
                    "last_update_login decimal," +
                    "item_group_id decimal," +
                    "recoil_safe_qty decimal," +
                    "recoil_call_qty decimal," +
                    "mo_split_flag string," +
                    "recoil_flag string," +
                    "lot_flag string," +
                    "review_flag string," +
                    "des_long string," +
                    "source_item_id decimal," +
                    "elect_machine_brand string," +
                    "elect_machine_v string," +
                    "brake_b_v string," +
                    "indep_fan_v string," +
                    "protect_level string," +
                    "elect_machine_frequency string," +
                    "elect_machine_remark1 string," +
                    "elect_machine_remark2 string," +
                    "weight string," +
                    "`module` string," +
                    "case_model string," +
                    "elect_machine_model string," +
                    "install string," +
                    "speed_ratio string," +
                    "lengths string," +
                    "wide string," +
                    "high string," +
                    "attribute1 string," +
                    "attribute2 string," +
                    "attribute3 string," +
                    "attribute4 string," +
                    "attribute5 string," +
                    "attribute6 string," +
                    "attribute7 string," +
                    "attribute8 string," +
                    "attribute9 string," +
                    "attribute10 string," +
                    "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_supplychain_material_item_b',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_supplychain_material_item_b select " +
                "  ITEM_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", PLANT_ID" +
                ", ITEM_CODE" +
                ", PRIMARY_UOM" +
                ", DESIGN_CODE" +
                ", PLAN_CODE" +
                ", ITEM_IDENTIFY_CODE" +
                ", ITEM_TYPE" +
                ", MAKE_BUY_CODE" +
                ", SUPPLY_TYPE" +
                ", KEY_COMPONENT_FLAG" +
                ", SCHEDULE_FLAG" +
                ", MAKE_TO_ORDER_FLAG" +
                ", PROD_LINE_RULE" +
                ", ITEM_SIZE" +
                ", UNIT_WEIGHT" +
                ", ENABLE_FLAG" +
                ", PRE_PROCESSING_LEAD_TIME" +
                ", PROCESSING_LEAD_TIME" +
                ", POST_PROCESSING_LEAD_TIME" +
                ", SAFETY_LEAD_TIME" +
                ", EXCEED_LEAD_TIME" +
                ", DEMAND_TIME_FENCE" +
                ", RELEASE_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", DEMAND_MERGE_TIME_FENCE" +
                ", SUPPLY_MERGE_TIME_FENCE" +
                ", SAFETY_STOCK_METHOD" +
                ", SAFETY_STOCK_PERIOD" +
                ", SAFETY_STOCK_VALUE" +
                ", MAX_STOCK_QTY" +
                ", MIN_STOCK_QTY" +
                ", PRODUCT_CAPACITY_TIME_FENCE" +
                ", PRODUCT_CAPACITY" +
                ", ASSEMBLY_SHRINKAGE" +
                ", ECONOMIC_LOT_SIZE" +
                ", ECONOMIC_SPLIT_PARAMETER" +
                ", MIN_ORDER_QTY" +
                ", FIXED_LOT_MULTIPLE" +
                ", PACK_QTY" +
                ", SEQUENCE_LOT_CONTROL" +
                ", ISSUE_CONTROL_TYPE" +
                ", ISSUE_CONTROL_QTY" +
                ", COMPLETE_CONTROL_TYPE" +
                ", COMPLETE_CONTROL_QTY" +
                ", EXPIRE_CONTROL_FLAG" +
                ", EXPIRE_DAYS" +
                ", RELEASE_CONCURRENT_RULE" +
                ", CID" +
                ", LAST_UPDATE_LOGIN" +
                ", ITEM_GROUP_ID" +
                ", RECOIL_SAFE_QTY" +
                ", RECOIL_CALL_QTY" +
                ", MO_SPLIT_FLAG" +
                ", RECOIL_FLAG" +
                ", LOT_FLAG" +
                ", REVIEW_FLAG" +
                ", DES_LONG" +
                ", SOURCE_ITEM_ID" +
                ", ELECT_MACHINE_BRAND" +
                ", ELECT_MACHINE_V" +
                ", BRAKE_B_V" +
                ", INDEP_FAN_V" +
                ", PROTECT_LEVEL" +
                ", ELECT_MACHINE_FREQUENCY" +
                ", ELECT_MACHINE_REMARK1" +
                ", ELECT_MACHINE_REMARK2" +
                ", WEIGHT" +
                ", `MODULE`" +
                ", CASE_MODEL" +
                ", ELECT_MACHINE_MODEL" +
                ", INSTALL" +
                ", SPEED_RATIO" +
                ", LENGTHS" +
                ", WIDE" +
                ", HIGH" +
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
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_item_b");

    }
}
