package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMesManagementEmployeeUser {
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
                "hdfs://10.0.0.50:8020/ck/ods_mes_management_employee_user"
        );
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 3. 利用FlinkCDC读取MES数据并用FlinkSql封装成动态表
        String sourceSql = "" +
                "CREATE TABLE ods_mes_management_employee_user ( " +
                "USER_ID INT," +
                "USER_NAME STRING," +
                "HFWK_PASSWORD STRING," +
                "USER_PASSWORD STRING," +
                "PASSWORD_TIMESTAMP TIMESTAMP," +
                "PASSWORD_EXPIRATION INT," +
                "LOGIN_EXPIRATION INT," +
                "ACTIVE_EXPIRATION INT," +
                "LAST_LOGON_TIMESTAMP TIMESTAMP," +
                "START_TIMESTAMP TIMESTAMP," +
                "END_TIMESTAMP TIMESTAMP," +
                "DESCRIPTION STRING," +
                "CID INT," +
                "CREATION_TIMESTAMP TIMESTAMP," +
                "CREATED_BY INT," +
                "LAST_UPTIMESTAMPD_BY INT," +
                "LAST_UPTIMESTAMP_TIMESTAMP TIMESTAMP," +
                "LAST_UPTIMESTAMP_LOGIN INT," +
                "ATTRIBUTE_CATEGORY STRING," +
                "ATTRIBUTE1 STRING," +
                "ATTRIBUTE2 STRING," +
                "ATTRIBUTE3 STRING," +
                "ATTRIBUTE4 STRING," +
                "ATTRIBUTE5 STRING," +
                "ATTRIBUTE6 STRING," +
                "ATTRIBUTE7 STRING," +
                "ATTRIBUTE8 STRING," +
                "ATTRIBUTE9 STRING," +
                "ATTRIBUTE10 STRING," +
                "ATTRIBUTE11 STRING," +
                "ATTRIBUTE12 STRING," +
                "ATTRIBUTE13 STRING," +
                "ATTRIBUTE14 STRING," +
                "ATTRIBUTE15 STRING," +
                "PASSWORD_ERROR_TIMES INT," +
                "PASSWORD_RESET_FLAG STRING," +
                "AUTO_START_MENU_ID INT," +
                "LOGIN_COUNT INT," +
                "DEPARTMENT STRING" +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = '172.16.10.57', " +
                " 'port' = '1521', " +
                " 'username' = 'hcms', " +
                " 'password' = 'hcmsprod', " +
                " 'database-name' = 'MESPROD', " +
                " 'schema-name' = 'HCMS'," +
                " 'table-name' = 'HFWK_USERS', " +
                " 'scan.startup.mode' = 'latest-offset'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sourceSql);

        //TODO 4. 读取doris的表信息到临时表中形成动态表
        String destinationSql = "CREATE TABLE doris_ods_mes_management_employee_user (\n" +
                " USER_ID string " +
                ", USER_NAME string" +
                ", HFWK_PASSWORD string" +
                ", USER_PASSWORD string" +
                ", PASSWORD_TIMESTAMP TIMESTAMP" +
                ", PASSWORD_EXPIRATION string"+
                ", LOGIN_EXPIRATION string"+
                ", ACTIVE_EXPIRATION string"+
                ", LAST_LOGON_TIMESTAMP TIMESTAMP"+
                ", START_TIMESTAMP TIMESTAMP"+
                ", END_TIMESTAMP TIMESTAMP"+
                ", DESCRIPTION string"+
                ", CID string"+
                ", CREATION_TIMESTAMP TIMESTAMP"+
                ", CREATED_BY string"+
                ", LAST_UPTIMESTAMPD_BY string"+
                ", LAST_UPTIMESTAMP_TIMESTAMP TIMESTAMP"+
                ", LAST_UPTIMESTAMP_LOGIN string"+
                ", ATTRIBUTE_CATEGORY String"+
                ", PASSWORD_ERROR_TIMES string"+
                ", PASSWORD_RESET_FLAG string"+
                ", AUTO_START_MENU_ID string"+
                ", LOGIN_COUNT string"+
                ", DEPARTMENT string"+
                ", UPTIMESTAMP_TIMESTAMPTIME TIMESTAMP" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.ods_mes_management_employee_user',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);

        //TODO 5. 将查询的结果插入到doris中（流式插入）
        tableEnv.executeSql("INSERT INTO doris_ods_mes_management_employee_user SELECT\n" +
                "  cast(USER_ID as string) USER_ID\n" +
                ", USER_NAME\n" +
                ", HFWK_PASSWORD\n" +
                ", USER_PASSWORD\n" +
                ", PASSWORD_TIMESTAMP\n" +
                ", cast(PASSWORD_EXPIRATION as string) PASSWORD_EXPIRATION\n" +
                ", cast(LOGIN_EXPIRATION as string) LOGIN_EXPIRATION\n" +
                ", cast(ACTIVE_EXPIRATION as string) ACTIVE_EXPIRATION\n" +
                ", LAST_LOGON_TIMESTAMP\n" +
                ", START_TIMESTAMP\n" +
                ", END_TIMESTAMP\n" +
                ", DESCRIPTION\n" +
                ", cast(CID as string) CID\n" +
                ", CREATION_TIMESTAMP\n" +
                ", cast(CREATED_BY as string) CREATE_BY\n" +
                ", cast(LAST_UPTIMESTAMPD_BY as string) LAST_UPTIMESTAMPD_BY\n" +
                ", LAST_UPTIMESTAMP_TIMESTAMP\n" +
                ", cast(LAST_UPTIMESTAMP_LOGIN as string) LAST_UPTIMESTAMP_LOGIN\n" +
                ", ATTRIBUTE_CATEGORY\n" +
                ", cast(PASSWORD_ERROR_TIMES as string) PASSWORD_ERROR_TIMES\n" +
                ", PASSWORD_RESET_FLAG\n" +
                ", cast(AUTO_START_MENU_ID as STRING) AUTO_START_MENU_ID\n" +
                ", cast(LOGIN_COUNT as string) LOGIN_COUNT\n" +
                ", DEPARTMENT" +
                ",CURRENT_TIMESTAMP from ods_mes_management_employee_user");

    }
}
