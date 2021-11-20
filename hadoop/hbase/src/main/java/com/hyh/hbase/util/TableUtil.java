package com.hyh.hbase.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 表操作工具类
 * 表操作的增删查改
 */
public class TableUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(TableUtil.class);

    /**
     * 列出所有的表
     *
     * @param connection
     * @return
     * @throws IOException
     */
    public static List<String> listTables(Connection connection) throws IOException {
        List<String> list = new ArrayList<String>();
        Admin admin = connection.getAdmin();
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor :
                hTableDescriptors) {
            list.add(hTableDescriptor.getNameAsString());
        }
        admin.close();
        return list;
    }

    /**
     * 列出具体的namespace中的所有table
     *
     * @param connection
     * @param nameSpace
     * @return
     * @throws IOException
     */
    public static List<String> listTables(Connection connection, String nameSpace) throws IOException {
        List<String> list = new ArrayList<String>();
        Admin admin = connection.getAdmin();
        HTableDescriptor[] hTableDescriptors = admin.listTables(nameSpace);
        for (HTableDescriptor hTableDescriptor :
                hTableDescriptors) {
            list.add(hTableDescriptor.getNameAsString());
        }
        admin.close();
        return list;
    }

    /**
     * 根据正则Pattern匹配table
     *
     * @param connection
     * @param pattern
     * @return
     * @throws IOException
     */
    public static List<String> listTables(Connection connection, Pattern pattern) throws IOException {
        List<String> list = new ArrayList<String>();
        Admin admin = connection.getAdmin();
        HTableDescriptor[] hTableDescriptors = admin.listTables(pattern);
        for (HTableDescriptor hTableDescriptor :
                hTableDescriptors) {
            list.add(hTableDescriptor.getNameAsString());
        }
        admin.close();
        return list;
    }

    /**
     * 判断表名是否存在
     *
     * @param connection
     * @param nameSpace
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean tableIsExists(Connection connection, String nameSpace, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        boolean tableExists = admin.tableExists(getTableName(nameSpace, tableName));
        admin.close();
        return tableExists;
    }

    public static TableName getTableName(String nameSpace, String tableName) {
        if (StringUtils.isBlank(tableName)) {
            LOGGER.error("表名不能为空");
            return null;
        }
        return TableName.valueOf(nameSpace, tableName);
    }

    /**
     * 创建表
     *
     * @param connection
     * @param nameSpace
     * @param table
     * @param cfs
     * @return
     */
    public static boolean createTable(Connection connection, String nameSpace, String table, String... cfs) throws IOException {
        //获取TableName对象
        TableName tableName = getTableName(nameSpace, table);
        //判断表是否存在
        if (tableIsExists(connection, nameSpace, table)) {
            LOGGER.error("表已存在");
            return false;
        }
        if (cfs.length < 1) {
            LOGGER.error("请输入至少一个列族");
            return false;
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String cf :
                cfs) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        Admin admin = connection.getAdmin();
        admin.createTable(hTableDescriptor);
        admin.close();
        return true;
    }

    /**
     * 删除表
     * @param connection
     * @param nameSpace
     * @param table
     * @return
     */
    public static boolean dropTable(Connection connection, String nameSpace, String table) throws IOException {
        //获取TableName对象
        TableName tableName = getTableName(nameSpace, table);
        //判断表是否存在
        if (!tableIsExists(connection, nameSpace, table)) {
            LOGGER.error("表不存在，不能删除");
            return false;
        }
        Admin admin = connection.getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        return true;
    }

}
