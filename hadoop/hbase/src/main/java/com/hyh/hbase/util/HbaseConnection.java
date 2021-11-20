package com.hyh.hbase.util;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBase连接工具类
 */
public class HbaseConnection {
    private static Connection connection = null;

    private HbaseConnection(Connection connection){

    }

    public static Connection getConnection() throws IOException {
        if (connection == null) {
            connection = ConnectionFactory.createConnection();
        }
        return connection;
    }

    public static void closeConnection(Connection connection) throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

//    public Connection getConnection() throws IOException {
//        Connection connection = ConnectionFactory.createConnection();
//        return connection;
//    }
}
