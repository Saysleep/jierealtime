package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesSupplychainMaterialRequirement {
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
                checkpointAddress + "ods_mes_supplychain_material_requirement"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_supplychain_material_requirement ( " +
                    "CREATED_DATE      TIMESTAMP," +
                    "CREATED_BY        INT," +
                    "LAST_UPDATE_BY    INT," +
                    "LAST_UPDATE_DATE  TIMESTAMP," +
                    "LAST_UPDATE_LOGIN INT," +
                    "KID               INT," +
                    "EO_ID             INT," +
                    "COMPONET_PLANT_ID INT," +
                    "COMPONET_ITEM_ID  INT," +
                    "UOM               STRING," +
                    "BOM_USAGE         INT," +
                    "DEMAND_QTY        INT," +
                    "ISSUED_QTY        INT," +
                    "MTL_SCRAPED_QTY   INT," +
                    "OP_SCRAPED_QTY    INT," +
                    "SUPPLY_TYPE       STRING," +
                    "WAREHOUSE_CODE    STRING," +
                    "LOCATOR_CODE      STRING," +
                    "CID               INT," +
                    "RECOIL_FLAG       STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HME_EO_REQUIREMENT', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_supplychain_material_requirement (\n" +
                "KID STRING," +
                "CREATED_DATE timestamp," +
                "CREATED_BY STRING," +
                "LAST_UPDATE_BY STRING," +
                "LAST_UPDATE_DATE timestamp," +
                "LAST_UPDATE_LOGIN STRING," +
                "EO_ID STRING," +
                "COMPONET_PLANT_ID STRING," +
                "COMPONET_ITEM_ID STRING," +
                "UOM STRING," +
                "BOM_USAGE STRING," +
                "DEMAND_QTY STRING," +
                "ISSUED_QTY STRING," +
                "MTL_SCRAPED_QTY STRING," +
                "OP_SCRAPED_QTY STRING," +
                "SUPPLY_TYPE STRING," +
                "WAREHOUSE_CODE STRING," +
                "LOCATOR_CODE STRING," +
                "CID STRING," +
                "RECOIL_FLAG STRING," +
                "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_supplychain_material_requirement',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_supplychain_material_requirement select " +
                    "  cast(KID as STRING) KID" +
                    ", CREATED_DATE" +
                    ", cast(CREATED_BY as STRING) CREATED_BY" +
                    ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY" +
                    ", LAST_UPDATE_DATE" +
                    ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN" +
                    ", cast(EO_ID as STRING) EO_ID" +
                    ", cast(COMPONET_PLANT_ID as STRING) COMPONET_PLANT_ID" +
                    ", cast(COMPONET_ITEM_ID as STRING) COMPONET_ITEM_ID" +
                    ", UOM" +
                    ", cast(BOM_USAGE as STRING) BOM_USAGE" +
                    ", cast(DEMAND_QTY as STRING) DEMAND_QTY" +
                    ", cast(ISSUED_QTY as STRING) ISSUED_QTY" +
                    ", cast(MTL_SCRAPED_QTY as STRING) MTL_SCRAPED_QTY" +
                    ", cast(OP_SCRAPED_QTY as STRING) OP_SCRAPED_QTY" +
                    ", SUPPLY_TYPE" +
                    ", WAREHOUSE_CODE" +
                    ", LOCATOR_CODE" +
                    ", cast(CID as STRING) CID" +
                    ", RECOIL_FLAG" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_requirement");
    }
}
