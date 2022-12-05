package com.jie.bigdata.realtime.app.ods;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.jie.bigdata.realtime.utils.DruidDSUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class GetImportSql {

    public static HashMap<String,String> getOracleColumn() throws SQLException {
        //获取druid连接
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection con = null;
        try {
            con = dataSource.getConnection();
        } catch (SQLException throwables) {
            System.out.println("从 Druid 连接池获取连接对象异常");
        }

        //
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from config.realtime_config");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
            System.out.println(resultSet.getString(2));
            System.out.println(resultSet.getString(3));
        }

        con.close();
        return null;
    }

    public static void main(String[] args) throws SQLException {
        getOracleColumn();
    }
}
