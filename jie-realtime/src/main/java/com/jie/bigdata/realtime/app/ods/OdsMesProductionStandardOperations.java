package com.jie.bigdata.realtime.app.ods;

import static com.jie.bigdata.realtime.utils.Constant.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMesProductionStandardOperations {
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
                checkpointAddress + "ods_mes_production_prodline_calendar"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql1 = "" +
                "CREATE TABLE ods_mes_production_standard_operations1 ( " +
                    "CREATION_DATE          TIMESTAMP," +
                    "CREATED_BY             INT," +
                    "LAST_UPDATED_BY        INT," +
                    "LAST_UPDATE_DATE       TIMESTAMP," +
                    "LAST_UPDATE_LOGIN      INT," +
                    "STANDARD_OP_ID         INT," +
                    "PLANT_ID               INT," +
                    "CODE                   STRING," +
                    "KEY_OP_FLAG            STRING," +
                    "MOVE_TYPE              STRING," +
                    "INSPECT_TYPE           STRING," +
                    "CHARGE_TYPE            STRING," +
                    "ENABLE_FLAG            STRING," +
                    "PROCESS_TIME           INT," +
                    "STANDARD_WORK_TIME     INT," +
                    "OPERATION_DOCUMENT     STRING," +
                    "PROCESS_PROGRAM        STRING," +
                    "CID                    INT," +
                    "SELF_TEST_FLAG         STRING," +
                    "FIRST_TEST_FLAG        STRING," +
                    "HEAT_TREATMENT_FLAG    STRING," +
                    "HEAT_TREATMENT_OS_FLAG STRING," +
                    "ATTRIBUTE1             STRING," +
                    "ATTRIBUTE2             STRING," +
                    "ATTRIBUTE3             STRING," +
                    "ATTRIBUTE4             STRING," +
                    "ATTRIBUTE5             STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_STANDARD_OP_B', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        String sourceSql2 = ""+
                "CREATE TABLE ods_mes_production_standard_operations2 ( " +
                    "CREATION_DATE     timestamp," +
                    "CREATED_BY        int," +
                    "LAST_UPDATED_BY   int," +
                    "LAST_UPDATE_DATE  timestamp," +
                    "LAST_UPDATE_LOGIN int," +
                    "STANDARD_OP_ID    int," +
                    "DESCRIPTION       string," +
                    "`LANGUAGE`          string," +
                    "SOURCE_LANGUAGE   string" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_STANDARD_OP_TL', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql1);
        tableEnv.executeSql(sourceSql2);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_standard_operations (\n" +
                    "STANDARD_OP_ID int," +
                    "CREATION_DATE timestamp," +
                    "CREATED_BY string," +
                    "LAST_UPDATED_BY string," +
                    "LAST_UPDATE_DATE timestamp," +
                    "LAST_UPDATE_LOGIN string," +
                    "PLANT_ID string," +
                    "CODE string," +
                    "KEY_OP_FLAG string," +
                    "MOVE_TYPE string," +
                    "INSPECT_TYPE string," +
                    "CHARGE_TYPE string," +
                    "ENABLE_FLAG string," +
                    "PROCESS_TIME string," +
                    "STANDARD_WORK_TIME string," +
                    "OPERATION_DOCUMENT string," +
                    "PROCESS_PROGRAM string," +
                    "CID string," +
                    "DESCRIPTION string," +
                    "SELF_TEST_FLAG string," +
                    "FIRST_TEST_FLAG string," +
                    "HEAT_TREATMENT_FLAG string," +
                    "HEAT_TREATMENT_OS_FLAG string," +
                    "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_standard_operations',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_standard_operations select " +
                "b.`STANDARD_OP_ID`," +
                "b.`CREATION_DATE`," +
                "cast(b.`CREATED_BY` as string)," +
                "cast(b.`LAST_UPDATED_BY` as string)," +
                "b.`LAST_UPDATE_DATE`," +
                "cast(b.`LAST_UPDATE_LOGIN` as string)," +
                "cast(b.`PLANT_ID` as string)," +
                "b.`CODE`," +
                "b.`KEY_OP_FLAG`," +
                "b.`MOVE_TYPE`," +
                "b.`INSPECT_TYPE`," +
                "b.`CHARGE_TYPE`," +
                "b.`ENABLE_FLAG`," +
                "cast(b.`PROCESS_TIME` as string)," +
                "cast(b.`STANDARD_WORK_TIME` as string)," +
                "b.`OPERATION_DOCUMENT`," +
                "b.`PROCESS_PROGRAM`," +
                "cast(b.`CID` as string)," +
                "t.`DESCRIPTION`," +
                "b.`SELF_TEST_FLAG`," +
                "b.`FIRST_TEST_FLAG`," +
                "b.`HEAT_TREATMENT_FLAG`," +
                "b.`HEAT_TREATMENT_OS_FLAG` " +
                ", CURRENT_TIMESTAMP FROM ods_mes_production_standard_operations1 b," +
                "ods_mes_production_standard_operations2 t " +
                "WHERE b.STANDARD_OP_ID = t.STANDARD_OP_ID " +
                "AND t.`LANGUAGE` = 'ZHS'");
    }
}