package com.hyh.hbase.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 数据操作工具类
 * 数据的增删查改
 */
public class DataUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(DataUtil.class);

    public static Table getTable(Connection connection, String nameSpace, String table) throws IOException {
        TableName tableName = TableUtil.getTableName(nameSpace, table);
        if (tableName == null) {
            return null;
        }
        return connection.getTable(tableName);
    }

    /**
     * 添加一行数据
     *
     * @param connection
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param cf
     * @param colName
     * @param value
     * @throws IOException
     */
    public static void put(Connection connection, String nameSpace, String tableName, String rowKey, String cf, String colName, String value) throws IOException {
        Table table = getTable(connection, nameSpace, tableName);
        if (table == null) {
            return;
        }
        if (StringUtils.isBlank(rowKey)) {
            LOGGER.error("rowKey必填");
            return;
        }
        if (StringUtils.isBlank(cf)) {
            LOGGER.error("列族必填");
            return;
        }
        if (StringUtils.isBlank(colName)) {
            LOGGER.error("列名必填");
            return;
        }
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 查询rowKey下的所有数据
     *
     * @param connection
     * @param nameSpace
     * @param tableName
     * @param rowKey
     */
    public static void get(Connection connection, String nameSpace, String tableName, String rowKey) throws IOException {
        Table table = getTable(connection, nameSpace, tableName);
        if (table == null) {
            return;
        }
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        parseResult(result);
        table.close();
    }

    public static void parseResult(Result result) {
        if (result == null) {
            return;
        }
        Cell[] cells = result.rawCells();
        for (Cell cell :
                cells) {
            System.out.println("rowKey: " + Bytes.toString(CellUtil.cloneRow(cell)) +
                    " 列族: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    " 列: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    " 值: " + Bytes.toString(CellUtil.cloneValue(cell)));

        }
    }

    /**
     * 查询所有数据
     *
     * @param connection
     * @param nameSpace
     * @param tableName
     */
    public static void scan(Connection connection, String nameSpace, String tableName) throws IOException {
        Table table = getTable(connection, nameSpace, tableName);
        if (table == null) {
            return;
        }
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result :
                scanner) {
            parseResult(result);
        }
        table.close();
    }

    /**
     * 删除一行数据
     *
     * @param connection
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static boolean delete(Connection connection, String nameSpace, String tableName, String rowKey) throws IOException {
        Table table = getTable(connection, nameSpace, tableName);
        if (table == null) {
            return false;
        }
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
        return true;
    }

    /**
     * 开启write buffer写缓存的put
     *
     * @param connection
     * @param nameSpace
     * @param tableName
     * @param rowKey
     * @param cf
     * @param colName
     * @param value
     */
    public static void putWithSetAutoFlushTo(Connection connection, String nameSpace, String tableName, String rowKey, String cf, String colName, String value) throws IOException {
        TableName tableName1 = TableUtil.getTableName(nameSpace, tableName);
        HTable hTable = new HTable(tableName1, connection);
        LOGGER.error("是否禁用写缓存==" + hTable.isAutoFlush());
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(colName), Bytes.toBytes(value));
        hTable.put(put);
        hTable.setAutoFlushTo(false);
        LOGGER.error("是否禁用写缓存==" + hTable.isAutoFlush());
        hTable.flushCommits();
        hTable.close();
    }
}
