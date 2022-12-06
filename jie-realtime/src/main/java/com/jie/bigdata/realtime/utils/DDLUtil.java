package com.jie.bigdata.realtime.utils;

public class DDLUtil {
    public static String getDorisDDL(String table){
        return  "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'realtime_database." + table +"',\n" +
                "      'sink.enable-delete' = 'true',\n" +
                "       'doris.batch.size' = '2048',\n" +
                //"       'sink.enable-2pc' = 'true',\n" +
                //"       'sink.label-prefix' = 'doris_mes',\n" +
                //"       'sink.properties.format' = 'json'," +
                //"       'sink.properties.read_json_by_line' = 'true'," +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
    }

    public static String getOracleDDL(String table){
        return " WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                        " 'hostname' = '172.16.10.57', " +
                        " 'port' = '1521', " +
                        " 'username' = 'hcms', " +
                        " 'password' = 'hcmsprod', " +
                        " 'database-name' = 'MESPROD', " +
                        " 'schema-name' = 'HCMS'," +
                        " 'table-name' = '" + table + "', " +
                        " 'scan.startup.mode' = 'latest-offset'," +
                        "'debezium.log.mining.strategy' = 'online_catalog'," +
                        "'debezium.log.mining.continuous.mine' = 'true'" +
                        ")";
    }
}
