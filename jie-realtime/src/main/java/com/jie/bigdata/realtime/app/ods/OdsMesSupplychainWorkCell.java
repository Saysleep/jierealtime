package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesSupplychainWorkCell {
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
                checkpointAddress + "ods_mes_supplychain_workcell"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_supplychain_workcell ( " +
                    "CREATED_DATE      TIMESTAMP," +
                    "CREATED_BY        INT," +
                    "LAST_UPDATE_BY    INT," +
                    "LAST_UPDATE_DATE  TIMESTAMP," +
                    "LAST_UPDATE_LOGIN INT," +
                    "WORKCELL_ID       INT," +
                    "WORKCELL_CODE     STRING," +
                    "DESCRIPTION       STRING," +
                    "WORKCELL_TYPE     STRING," +
                    "MERGE_FLAG        STRING," +
                    "GPS_INFO          STRING," +
                    "ENABLE_FLAG       STRING," +
                    "CID               INT," +
                    "WORKSHOP_ID       INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_WORKCELL', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_supplychain_workcell (\n" +
                    "WORKCELL_ID decimal," +
                    "CREATED_DATE TIMESTAMP," +
                    "CREATED_BY decimal," +
                    "LAST_UPDATE_BY decimal," +
                    "LAST_UPDATE_DATE TIMESTAMP," +
                    "LAST_UPDATE_LOGIN decimal," +
                    "WORKCELL_CODE STRING," +
                    "DESCRIPTION STRING," +
                    "WORKCELL_TYPE STRING," +
                    "MERGE_FLAG STRING," +
                    "GPS_INFO STRING," +
                    "ENABLE_FLAG STRING," +
                    "CID decimal," +
                    "WORKSHOP_ID decimal," +
                    "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_supplychain_workcell',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_supplychain_workcell select " +
                    "  WORKCELL_ID" +
                    ", CREATED_DATE" +
                    ", CREATED_BY" +
                    ", LAST_UPDATE_BY" +
                    ", LAST_UPDATE_DATE" +
                    ", LAST_UPDATE_LOGIN" +
                    ", WORKCELL_CODE" +
                    ", DESCRIPTION" +
                    ", WORKCELL_TYPE" +
                    ", MERGE_FLAG" +
                    ", GPS_INFO" +
                    ", ENABLE_FLAG" +
                    ", CID" +
                    ", WORKSHOP_ID" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_workcell");

    }
}
