package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesProductionProdlineMoveRecord {
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
                checkpointAddress + "ods_mes_production_prodline_move_record"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_move_record ( " + 
                    "CREATED_BY          INT," +
                    "CREATION_DATE       TIMESTAMP," +
                    "LAST_UPDATE_BY      INT," +
                    "LAST_UPDATE_DATE    TIMESTAMP," +
                    "LAST_UPDATE_LOGIN   INT," +
                    "KID                 INT," +
                    "PLANT_ID            INT," +
                    "BARCODE_NUMBER      STRING," +
                    "WORKCELL_ID         INT," +
                    "EO_ID               INT," +
                    "IN_STATION_DATE     TIMESTAMP," +
                    "IN_STATION_QTY      INT," +
                    "IN_STATION_USER_ID  INT," +
                    "OUT_STATION_DATE    TIMESTAMP," +
                    "OUT_STATION_QTY     INT," +
                    "OUT_STATION_USER_ID INT," +
                    "REMARK              STRING," +
                    "ATTRIBUTE1          STRING," +
                    "ATTRIBUTE2          STRING," +
                    "ATTRIBUTE3          STRING," +
                    "ATTRIBUTE4          STRING," +
                    "ATTRIBUTE5          STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HME_JIE_MOVE_RECORD', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_move_record (\n" +
                    "KID string," +
                    "CREATED_BY string," +
                    "CREATION_DATE timestamp," +
                    "LAST_UPDATE_BY string," +
                    "LAST_UPDATE_DATE timestamp," +
                    "LAST_UPDATE_LOGIN string," +
                    "PLANT_ID string," +
                    "BARCODE_NUMBER string," +
                    "WORKCELL_ID string," +
                    "EO_ID string," +
                    "IN_STATION_DATE timestamp," +
                    "IN_STATION_QTY string," +
                    "IN_STATION_USER_ID string," +
                    "OUT_STATION_DATE timestamp," +
                    "OUT_STATION_QTY string," +
                    "OUT_STATION_USER_ID string," +
                    "REMARK string," +
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
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_move_record',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_move_record select " +
                "  cast(KID as STRING) KID" +
                ", cast(CREATED_BY as STRING) CREATED_BY" +
                ", CREATION_DATE" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN" +
                ", cast(PLANT_ID as STRING) PLANT_ID" +
                ", BARCODE_NUMBER" +
                ", cast(WORKCELL_ID as STRING) WORKCELL_ID" +
                ", cast(EO_ID as STRING) EO_ID" +
                ", IN_STATION_DATE" +
                ", cast(IN_STATION_QTY as STRING) IN_STATION_QTY" +
                ", cast(IN_STATION_USER_ID as STRING) IN_STATION_USER_ID" +
                ", OUT_STATION_DATE" +
                ", cast(OUT_STATION_QTY as STRING) OUT_STATION_QTY" +
                ", cast(OUT_STATION_USER_ID as STRING) OUT_STATION_USER_ID" +
                ", REMARK" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_move_record");
    }
}
