package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMesProductionProdlineComponent {
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
                "hdfs://10.0.0.50:8020/ck/ods_mes_production_prodline_component"
        );
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_component ( " +
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
        tableEnv.sqlQuery("select * from ods_mes_production_prodline_component").execute().print();

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_component (\n" +
                "KID STRING,\n" +
                "CREATED_DATE timestamp,\n" +
                "CREATED_BY STRING,\n" +
                "LAST_UPDATE_BY STRING,\n" +
                "LAST_UPDATE_DATE timestamp,\n" +
                "LAST_UPDATE_LOGIN STRING,\n" +
                "EO_ID STRING,\n" +
                "COMPONET_PLANT_ID STRING,\n" +
                "COMPONET_ITEM_ID STRING,\n" +
                "UOM STRING,\n" +
                "BOM_USAGE STRING,\n" +
                "DEMAND_QTY STRING,\n" +
                "ISSUED_QTY STRING,\n" +
                "MTL_SCRAPED_QTY STRING,\n" +
                "OP_SCRAPED_QTY STRING,\n" +
                "SUPPLY_TYPE STRING,\n" +
                "WAREHOUSE_CODE STRING,\n" +
                "LOCATOR_CODE STRING,\n" +
                "CID STRING,\n" +
                "RECOIL_FLAG STRING,\n" +
                "update_datetime timestamp" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_component',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_component select " +
                " cast(KID as STRING) KID\n" +
                ", CREATED_DATE\n" +
                ", cast(CREATED_BY as STRING) CREATED_BY\n" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY\n" +
                ", LAST_UPDATE_DATE\n" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN\n" +
                ", cast(EO_ID as STRING) EO_ID\n" +
                ", cast(COMPONET_PLANT_ID as STRING) COMPONET_PLANT_ID\n" +
                ", cast(COMPONET_ITEM_ID as STRING) COMPONET_ITEM_ID\n" +
                ", UOM\n" +
                ", cast(BOM_USAGE as STRING) BOM_USAGE\n" +
                ", cast(DEMAND_QTY as STRING) DEMAND_QTY\n" +
                ", cast(ISSUED_QTY as STRING) ISSUED_QTY\n" +
                ", cast(MTL_SCRAPED_QTY as STRING) MTL_SCRAPED_QTY\n" +
                ", cast(OP_SCRAPED_QTY as STRING) OP_SCRAPED_QTY\n" +
                ", SUPPLY_TYPE\n" +
                ", WAREHOUSE_CODE\n" +
                ", LOCATOR_CODE\n" +
                ", cast(CID as STRING) CID\n" +
                ", RECOIL_FLAG , CURRENT_TIMESTAMP from ods_mes_production_prodline_component");
    }
}
