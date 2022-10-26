package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesSupplychainMaterialWarehouse {
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
                checkpointAddress + "ods_mes_supplychain_material_warehouse"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_supplychain_material_warehouse ( " +
                "CREATED_BY           INT," +
                "CREATION_DATE        TIMESTAMP," +
                "LAST_UPDATED_BY      INT," +
                "LAST_UPDATE_DATE     TIMESTAMP," +
                "LAST_UPDATE_LOGIN    INT," +
                "KID                  INT," +
                "PLANT_ID             INT," +
                "WAREHOUSE_CODE       STRING," +
                "DESCRIPTIONS         STRING," +
                "ENABLE_FLAG          STRING," +
                "WAREHOUSE_SHORT_CODE STRING," +
                "WAREHOUSE_TYPE       STRING," +
                "WMS_FLAG             STRING," +
                "ONHAND_FLAG          STRING," +
                "LOCATOR_FLAG         STRING," +
                "PLAN_FLAG            STRING," +
                "NEGATIVE_FLAG        STRING," +
                "CID                  INT," +
                "THING_LABEL_FLAG     STRING," +
                "WORKSHOP_ID          INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_WAREHOUSE', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_supplychain_material_warehouse (\n" +
                "KID int," +
                "CREATED_BY int," +
                "CREATION_DATE timestamp," +
                "LAST_UPDATED_BY int," +
                "LAST_UPDATE_DATE timestamp," +
                "LAST_UPDATE_LOGIN int," +
                "PLANT_ID int," +
                "WAREHOUSE_CODE string," +
                "DESCRIPTIONS string," +
                "ENABLE_FLAG string," +
                "WAREHOUSE_SHORT_CODE string," +
                "WAREHOUSE_TYPE string," +
                "WMS_FLAG string," +
                "ONHAND_FLAG string," +
                "LOCATOR_FLAG string," +
                "PLAN_FLAG string," +
                "NEGATIVE_FLAG string," +
                "CID int," +
                "THING_LABEL_FLAG string," +
                "WORKSHOP_ID int," +
                "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_supplychain_material_warehouse',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_supplychain_material_warehouse select " +
                        "  KID" +
                        ", CREATED_BY" +
                        ", CREATION_DATE" +
                        ", LAST_UPDATED_BY" +
                        ", LAST_UPDATE_DATE" +
                        ", LAST_UPDATE_LOGIN" +
                        ", PLANT_ID" +
                        ", WAREHOUSE_CODE" +
                        ", DESCRIPTIONS" +
                        ", ENABLE_FLAG" +
                        ", WAREHOUSE_SHORT_CODE" +
                        ", WAREHOUSE_TYPE" +
                        ", WMS_FLAG" +
                        ", ONHAND_FLAG" +
                        ", LOCATOR_FLAG" +
                        ", PLAN_FLAG" +
                        ", NEGATIVE_FLAG" +
                        ", CID" +
                        ", THING_LABEL_FLAG" +
                        ", WORKSHOP_ID" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_warehouse");

    }
}
