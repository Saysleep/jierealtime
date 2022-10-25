package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.jie.bigdata.realtime.utils.Constant.*;
import static com.jie.bigdata.realtime.utils.Constant.hadoopUserName;

public class OdsMesProductionProdlineExecutiveOrder {
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
                checkpointAddress + "ods_mes_production_prodline_executive_order"
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_production_prodline_executive_order (  " +
                "CREATED_DATE            TIMESTAMP," +
                "CREATED_BY              INT," +
                "LAST_UPDATE_BY          INT," +
                "LAST_UPDATE_DATE        TIMESTAMP," +
                "LAST_UPDATE_LOGIN       INT," +
                "EO_ID                   INT," +
                "EXECUTIVE_ORDER_NUM     STRING," +
                "WORK_ORDER_ID           INT," +
                "STATUS                  STRING," +
                "WORKCELL_ID             INT," +
                "PRIORITY                INT," +
                "START_TIME              TIMESTAMP," +
                "END_TIME                TIMESTAMP," +
                "QUANTITY                INT," +
                "UOM                     STRING," +
                "EO_TYPE                 STRING," +
                "KEY_OP_FLAG             STRING," +
                "MOVE_TYPE               STRING," +
                "INSPECT_TYPE            STRING," +
                "CHARGE_TYPE             STRING," +
                "COMPLETED_QTY           INT," +
                "IN_STATION_QTY          INT," +
                "OUT_STATION_QTY         INT," +
                "SCRAPED_QTY             INT," +
                "CID                     INT," +
                "VALIDATE_FLAG           STRING," +
                "MAKE_ORDER_ID           INT," +
                "OPERATION_SEQ_NUM       INT," +
                "DUTY_SCRAPED_QTY        INT," +
                "ITEM_SCRAPED_QTY        INT," +
                "REPAIR_QTY              INT," +
                "REPAIR_FLAG             STRING," +
                "COMPLETE_USER_ID        INT," +
                "DELIVERY_TICKET         STRING," +
                "WIP_BARCODE_QTY         INT," +
                "WIP_BARCODE_SCRAPED_QTY INT," +
                "REMARKS                 STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HME_EXECUTIVE_ORDER', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);
        //tableEnv.sqlQuery("select * from ods_mes_production_prodline_executive_order").execute().print();

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_production_prodline_executive_order (\n" +
                "EO_ID decimal," +
                "CREATED_DATE timestamp," +
                "CREATED_BY decimal," +
                "LAST_UPDATE_BY decimal," +
                "LAST_UPDATE_DATE timestamp," +
                "LAST_UPDATE_LOGIN decimal," +
                "EXECUTIVE_ORDER_NUM string," +
                "WORK_ORDER_ID decimal," +
                "STATUS string," +
                "WORKCELL_ID decimal," +
                "PRIORITY decimal," +
                "START_TIME timestamp," +
                "END_TIME timestamp," +
                "QUANTITY decimal," +
                "UOM string," +
                "EO_TYPE string," +
                "KEY_OP_FLAG string," +
                "MOVE_TYPE string," +
                "INSPECT_TYPE string," +
                "CHARGE_TYPE string," +
                "COMPLETED_QTY decimal," +
                "IN_STATION_QTY decimal," +
                "OUT_STATION_QTY decimal," +
                "SCRAPED_QTY decimal," +
                "CID decimal," +
                "VALIDATE_FLAG string," +
                "MAKE_ORDER_ID decimal," +
                "OPERATION_SEQ_NUM decimal," +
                "DUTY_SCRAPED_QTY decimal," +
                "ITEM_SCRAPED_QTY decimal," +
                "REPAIR_QTY decimal," +
                "REPAIR_FLAG string," +
                "COMPLETE_USER_ID decimal," +
                "DELIVERY_TICKET string," +
                "WIP_BARCODE_QTY decimal," +
                "WIP_BARCODE_SCRAPED_QTY decimal," +
                "REMARKS string," +
                "update_datetime timestamp" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_production_prodline_executive_order',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_production_prodline_executive_order select " +
                "  EO_ID\n" +
                ", CREATED_DATE\n" +
                ", CREATED_BY\n" +
                ", LAST_UPDATE_BY\n" +
                ", LAST_UPDATE_DATE\n" +
                ", LAST_UPDATE_LOGIN\n" +
                ", EXECUTIVE_ORDER_NUM\n" +
                ", WORK_ORDER_ID\n" +
                ", STATUS\n" +
                ", WORKCELL_ID\n" +
                ", PRIORITY\n" +
                ", START_TIME\n" +
                ", END_TIME\n" +
                ", QUANTITY\n" +
                ", UOM\n" +
                ", EO_TYPE\n" +
                ", KEY_OP_FLAG\n" +
                ", MOVE_TYPE\n" +
                ", INSPECT_TYPE\n" +
                ", CHARGE_TYPE\n" +
                ", COMPLETED_QTY\n" +
                ", IN_STATION_QTY\n" +
                ", OUT_STATION_QTY\n" +
                ", SCRAPED_QTY\n" +
                ", CID\n" +
                ", VALIDATE_FLAG\n" +
                ", MAKE_ORDER_ID\n" +
                ", OPERATION_SEQ_NUM\n" +
                ", DUTY_SCRAPED_QTY\n" +
                ", ITEM_SCRAPED_QTY\n" +
                ", REPAIR_QTY\n" +
                ", REPAIR_FLAG\n" +
                ", COMPLETE_USER_ID\n" +
                ", DELIVERY_TICKET\n" +
                ", WIP_BARCODE_QTY\n" +
                ", WIP_BARCODE_SCRAPED_QTY\n" +
                ", REMARKS, CURRENT_TIMESTAMP FROM ods_mes_production_prodline_executive_order");

    }
}
