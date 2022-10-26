package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesProductionWipBarcode {
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
                checkpointAddress + "ods_mes_production_wip_barcode"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_wip_barcode ( " +
                    "CREATED_BY                INT," +
                    "CREATION_DATE             TIMESTAMP," +
                    "LAST_UPDATED_BY           INT," +
                    "LAST_UPDATE_DATE          TIMESTAMP," +
                    "LAST_UPDATE_LOGIN         INT," +
                    "KID                       INT," +
                    "WIP_BARCODE               STRING," +
                    "EO_ID                     INT," +
                    "PLANT_ID                  INT," +
                    "ITEM_ID                   INT," +
                    "QTY                       INT," +
                    "STATUS                    STRING," +
                    "PACKING_LIST              STRING," +
                    "PACKING_LIST_CREATED_BY   INT," +
                    "PACKING_LIST_CREATED_DATE TIMESTAMP," +
                    "PACKING_LIST_PRINTED_BY   INT," +
                    "PACKING_LIST_PRINTED_DATE TIMESTAMP," +
                    "PARENT_WIP_BARCODE        STRING," +
                    "INSPECT_STATUS            STRING," +
                    "FURNACE_ID                INT," +
                    "OUTSOURCING_STATUS        STRING," +
                    "REWORK_MARK               STRING," +
                    "LOCATOR_CODE              STRING," +
                    "CARRIER_CODE              STRING," +
                    "DISMANTLING               STRING," +
                    "CID                       INT," +
                    "ATTRIBUTE1                STRING," +
                    "ATTRIBUTE2                STRING," +
                    "ATTRIBUTE3                STRING," +
                    "ATTRIBUTE4                STRING," +
                    "ATTRIBUTE5                STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HME_JIE_WIP_BARCODE', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_wip_barcode (\n" +
                    "KID decimal," +
                    "CREATED_BY decimal," +
                    "CREATION_DATE timestamp," +
                    "LAST_UPDATED_BY decimal," +
                    "LAST_UPDATE_DATE timestamp," +
                    "LAST_UPDATE_LOGIN decimal," +
                    "WIP_BARCODE string," +
                    "EO_ID decimal," +
                    "PLANT_ID decimal," +
                    "material_id decimal," +
                    "QTY decimal," +
                    "STATUS string," +
                    "PACKING_LIST string," +
                    "PACKING_LIST_CREATED_BY decimal," +
                    "PACKING_LIST_CREATED_DATE timestamp," +
                    "PACKING_LIST_PRINTED_BY decimal," +
                    "PACKING_LIST_PRINTED_DATE timestamp," +
                    "PARENT_WIP_BARCODE string," +
                    "INSPECT_STATUS string," +
                    "FURNACE_ID decimal," +
                    "OUTSOURCING_STATUS string," +
                    "REWORK_MARK string," +
                    "LOCATOR_CODE string," +
                    "CARRIER_CODE string," +
                    "DISMANTLING string," +
                    "CID decimal," +
                    "ATTRIBUTE1 string," +
                    "ATTRIBUTE2 string," +
                    "ATTRIBUTE3 string," +
                    "ATTRIBUTE4 string," +
                    "ATTRIBUTE5 string," +
                    "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_wip_barcode',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_wip_barcode select " +
                "  KID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", WIP_BARCODE" +
                ", EO_ID" +
                ", PLANT_ID" +
                ", ITEM_ID" +
                ", QTY" +
                ", STATUS" +
                ", PACKING_LIST" +
                ", PACKING_LIST_CREATED_BY" +
                ", PACKING_LIST_CREATED_DATE" +
                ", PACKING_LIST_PRINTED_BY" +
                ", PACKING_LIST_PRINTED_DATE" +
                ", PARENT_WIP_BARCODE" +
                ", INSPECT_STATUS" +
                ", FURNACE_ID" +
                ", OUTSOURCING_STATUS" +
                ", REWORK_MARK" +
                ", LOCATOR_CODE" +
                ", CARRIER_CODE" +
                ", DISMANTLING" +
                ", CID" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", CURRENT_TIMESTAMP from ods_mes_production_wip_barcode");

    }
}
