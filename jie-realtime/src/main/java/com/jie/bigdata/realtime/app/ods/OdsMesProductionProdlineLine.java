package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMesProductionProdlineLine {
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
                "hdfs://10.0.0.50:8020/ck/ods_mes_production_prodline_line"
        );
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_line ( " +
                "CREATED_BY                  INT," +
                "CREATION_DATE               TIMESTAMP," +
                "LAST_UPDATED_BY             INT," +
                "LAST_UPDATE_DATE            TIMESTAMP," +
                "LAST_UPDATE_LOGIN           INT," +
                "SCHEDULE_REGION_ID          INT," +
                "PLANT_ID                    INT," +
                "PROD_LINE_GROUP_ID          INT," +
                "PROD_LINE_ID                INT," +
                "PROD_LINE_CODE              STRING," +
                "PROD_LINE_ALIAS             STRING," +
                "PROD_LINE_TYPE              STRING," +
                "DESCRIPTIONS                STRING," +
                "RATE_TYPE                   STRING," +
                "RATE                        INT," +
                "ACTIVITY                    INT," +
                "PLANNER                     INT," +
                "BUSINESS_AREA               STRING," +
                "ORDER_BY_CODE               STRING," +
                "ENABLE_FLAG                 STRING," +
                "ISSUED_WAREHOUSE_CODE       STRING," +
                "ISSUED_LOCATOR_CODE         STRING," +
                "FIX_TIME_FENCE              INT," +
                "SUPPLIER_ID                 INT," +
                "SUPPLIER_SITE_ID            INT," +
                "FORWARD_PLANNING_TIME_FENCE INT," +
                "MO_REFRESH_FLAG             STRING," +
                "COMPLETION_WAREHOUSE_CODE   STRING," +
                "COMPLETION_LOCATOR_CODE     STRING," +
                "FROZEN_TIME_FENCE           INT," +
                "ORDER_TIME_FENCE            INT," +
                "RELEASE_TIME_FENCE          INT," +
                "INVENTORY_WAREHOUSE_CODE    STRING," +
                "INVENTORY_LOCATOR_CODE      STRING," +
                "CID                         INT" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HCM_PRODUCTION_LINE', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);
        //tableEnv.sqlQuery("select * from ods_mes_production_prodline_line").execute().print();

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_line (\n" +
                "prod_line_id decimal," +
                "created_by decimal," +
                "creation_date timestamp," +
                "last_updated_by decimal," +
                "last_update_date timestamp," +
                "last_update_login decimal," +
                "schedule_region_id decimal," +
                "plant_id decimal," +
                "prod_line_group_id decimal," +
                "prod_line_code string," +
                "prod_line_alias string," +
                "prod_line_type string," +
                "descriptions string," +
                "rate_type string," +
                "rate decimal," +
                "activity decimal," +
                "planner decimal," +
                "business_area string," +
                "order_by_code string," +
                "enable_flag string," +
                "issued_warehouse_code string," +
                "issued_locator_code string," +
                "fix_time_fence decimal," +
                "supplier_id decimal," +
                "supplier_site_id decimal," +
                "forward_planning_time_fence decimal," +
                "mo_refresh_flag string," +
                "completion_warehouse_code string," +
                "completion_locator_code string," +
                "frozen_time_fence decimal," +
                "order_time_fence decimal," +
                "release_time_fence decimal," +
                "inventory_warehouse_code string," +
                "inventory_locator_code string," +
                "cid decimal," +
                "update_datetime timestamp" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_line',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);
        //tableEnv.sqlQuery("select * from doris_ods_mes_production_prodline_line").execute().print();

        //TODO 5. 将查询的结果插入到doris中（流式插入）
/**/    tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_line select " +
                "  PROD_LINE_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", SCHEDULE_REGION_ID" +
                ", PLANT_ID" +
                ", PROD_LINE_GROUP_ID" +
                ", PROD_LINE_CODE" +
                ", PROD_LINE_ALIAS" +
                ", PROD_LINE_TYPE" +
                ", DESCRIPTIONS" +
                ", RATE_TYPE" +
                ", RATE" +
                ", ACTIVITY" +
                ", PLANNER" +
                ", BUSINESS_AREA" +
                ", ORDER_BY_CODE" +
                ", ENABLE_FLAG" +
                ", ISSUED_WAREHOUSE_CODE" +
                ", ISSUED_LOCATOR_CODE" +
                ", FIX_TIME_FENCE" +
                ", SUPPLIER_ID" +
                ", SUPPLIER_SITE_ID" +
                ", FORWARD_PLANNING_TIME_FENCE" +
                ", MO_REFRESH_FLAG" +
                ", COMPLETION_WAREHOUSE_CODE" +
                ", COMPLETION_LOCATOR_CODE" +
                ", FROZEN_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", RELEASE_TIME_FENCE" +
                ", INVENTORY_WAREHOUSE_CODE" +
                ", INVENTORY_LOCATOR_CODE" +
                ", CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_line");

    }
}
