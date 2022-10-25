package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesProductionProdlineWkcgRel {
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
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_wkcg_rel ( " +
                    "CREATED_DATE      TIMESTAMP," +
                    "CREATED_BY        INT," +
                    "LAST_UPDATE_BY    INT," +
                    "LAST_UPDATE_DATE  TIMESTAMP," +
                    "LAST_UPDATE_LOGIN INT," +
                    "REL_ID            INT," +
                    "PROD_LINE_ID      INT," +
                    "WORKCELL_ID       INT," +
                    "PRIORITY          INT," +
                    "ENABLE_FLAG       STRING," +
                    "CID               INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_PROD_LINE_WKCG_REL', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_wkcg_rel (\n" +
                    "REL_ID string," +
                    "CREATED_DATE timestamp," +
                    "CREATED_BY string," +
                    "LAST_UPDATE_BY string," +
                    "LAST_UPDATE_DATE timestamp," +
                    "LAST_UPDATE_LOGIN string," +
                    "PROD_LINE_ID string," +
                    "WORKCELL_ID string," +
                    "PRIORITY string," +
                    "ENABLE_FLAG string," +
                    "CID string," +
                    "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_wkcg_rel',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_wkcg_rel select " +
                "  cast(REL_ID as STRING) REL_ID" +
                ", CREATED_DATE" +
                ", cast(CREATED_BY as STRING) CREATED_BY" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN" +
                ", cast(PROD_LINE_ID as STRING) PROD_LINE_ID" +
                ", cast(WORKCELL_ID as STRING) WORKCELL_ID" +
                ", cast(PRIORITY as STRING) PRIORITY" +
                ", ENABLE_FLAG" +
                ", cast(CID as STRING) CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_wkcg_rel");
    }
}