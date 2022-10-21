package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMesProductionOperationSequence {
    public static void main(String[] args) {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://10.0.0.50:8020/ck/ods_mes_production_operation_sequence"
        );
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "CREATE TABLE ods_mes_production_operation_sequence ( " +
                "        CREATION_DATE      TIMESTAMP,\n" +
                "        CREATED_BY         INT,\n" +
                "        LAST_UPDATED_BY    INT,\n" +
                "        LAST_UPDATE_DATE   TIMESTAMP,\n" +
                "        LAST_UPDATE_LOGIN  INT,\n" +
                "        KID                INT,\n" +
                "        MAKE_ORDER_ID      INT,\n" +
                "        OPERATION_SEQ_NUM  INT,\n" +
                "        STANDARD_OP_ID     INT,\n" +
                "        ENABLE_FLAG        STRING,\n" +
                "        OPERATION_TIME1    INT,\n" +
                "        OPERATION_TIME2    INT,\n" +
                "        OPERATION_TIME3    INT,\n" +
                "        OPERATION_TIME4    INT,\n" +
                "        OPERATION_TIME5    INT,\n" +
                "        OPERATION_TIME6    INT,\n" +
                "        KEY_OP_FLAG        STRING,\n" +
                "        MOVE_TYPE          STRING,\n" +
                "        INSPECT_TYPE       STRING,\n" +
                "        CHARGE_TYPE        STRING,\n" +
                "        CID                INT,\n" +
                "        PLANT              INT,\n" +
                "        SEQUENCE           STRING,\n" +
                "        WORK_CENTER        STRING,\n" +
                "        EXECUTE_PLANT      INT,\n" +
                "        STANDARD_TXT       STRING,\n" +
                "        OPERATION_DES      STRING,\n" +
                "        OPERATION_LONG_TXT STRING,\n" +
                "        OUTSOURCE_FLAG     STRING,\n" +
                "        OPERATION_QTY      INT,\n" +
                "        LABOR_TIME         STRING,\n" +
                "        LABOR_UOM          STRING,\n" +
                "        MACHINE_TIME       STRING,\n" +
                "        MACHINE_UOM        STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HPS_MO_OPERATION_SEQUENCE', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);
        //tableEnv.sqlQuery("select * from ods_mes_production_operation_sequence limit 3").execute().print();

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_operation_sequence(\n" +
                " KID int,\n" +
                "CREATION_DATE timestamp,\n" +
                "CREATED_BY string,\n" +
                "LAST_UPDATED_BY string,\n" +
                "LAST_UPDATE_DATE timestamp,\n" +
                "LAST_UPDATE_LOGIN string,\n" +
                "MAKE_ORDER_ID int,\n" +
                "OPERATION_SEQ_NUM int,\n" +
                "STANDARD_OP_ID int,\n" +
                "ENABLE_FLAG string,\n" +
                "OPERATION_TIME1 string,\n" +
                "OPERATION_TIME2 string,\n" +
                "OPERATION_TIME3 string,\n" +
                "OPERATION_TIME4 string,\n" +
                "OPERATION_TIME5 string,\n" +
                "OPERATION_TIME6 string,\n" +
                "KEY_OP_FLAG string,\n" +
                "MOVE_TYPE string,\n" +
                "INSPECT_TYPE string,\n" +
                "CHARGE_TYPE string,\n" +
                "CID string,\n" +
                "PLANT string,\n" +
                "SEQUENCE string,\n" +
                "WORK_CENTER string,\n" +
                "EXECUTE_PLANT string,\n" +
                "STANDARD_TXT string,\n" +
                "OPERATION_DES string,\n" +
                "OPERATION_LONG_TXT string,\n" +
                "OUTSOURCE_FLAG string,\n" +
                "OPERATION_QTY string,\n" +
                "LABOR_TIME string,\n" +
                "LABOR_UOM string,\n" +
                "MACHINE_TIME string,\n" +
                "MACHINE_UOM string," +
                "update_datetime TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_operation_sequence',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_operation_sequence select " +
                "   KID,\n" +
                "  CREATION_DATE,\n" +
                " cast(CREATED_BY as string) CREATED_BY,\n" +
                " cast(LAST_UPDATED_BY as string) LAST_UPDATED_BY,\n" +
                " LAST_UPDATE_DATE,\n" +
                " cast(LAST_UPDATE_LOGIN as string) LAST_UPDATE_LOGIN,\n" +
                " MAKE_ORDER_ID,\n" +
                " OPERATION_SEQ_NUM,\n" +
                " STANDARD_OP_ID,\n" +
                " ENABLE_FLAG,\n" +
                " cast(OPERATION_TIME1 as string) OPERATION_TIME1,\n" +
                " cast(OPERATION_TIME2 as string) OPERATION_TIME2,\n" +
                " cast(OPERATION_TIME3 as string) OPERATION_TIME3,\n" +
                " cast(OPERATION_TIME4 as string) OPERATION_TIME4,\n" +
                " cast(OPERATION_TIME5 as string) OPERATION_TIME5,\n" +
                " cast(OPERATION_TIME6 as string) OPERATION_TIME6,\n" +
                " KEY_OP_FLAG,\n" +
                " MOVE_TYPE,\n" +
                " INSPECT_TYPE,\n" +
                " CHARGE_TYPE,\n" +
                " cast(CID as string) CID,\n" +
                " cast(PLANT as string) PLANT,\n" +
                " SEQUENCE,\n" +
                " WORK_CENTER,\n" +
                " cast(EXECUTE_PLANT as string) EXECUTE_PLANT,\n" +
                " STANDARD_TXT,\n" +
                " OPERATION_DES,\n" +
                " OPERATION_LONG_TXT,\n" +
                " OUTSOURCE_FLAG,\n" +
                " cast(OPERATION_QTY as string) OPERATION_QTY,\n" +
                " LABOR_TIME,\n" +
                " LABOR_UOM,\n" +
                " MACHINE_TIME,\n" +
                " MACHINE_UOM, " +
                " CURRENT_TIMESTAMP FROM ods_mes_production_operation_sequence");
    }
}
