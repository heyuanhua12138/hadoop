import com.hyh.hbase.util.HbaseConnection;
import com.hyh.hbase.util.NameSpaceUtil;
import com.hyh.hbase.util.TableUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

public class TestTable {
    private Connection connection;
    @org.junit.Test
    public void test1() throws IOException {
        Connection connection = HbaseConnection.getConnection();
        System.out.println(connection);
    }

    @Before
    public void init() throws IOException {
        connection = HbaseConnection.getConnection();
    }

    @After
    public void closeConnection() throws IOException {
        HbaseConnection.closeConnection(connection);
    }

    @org.junit.Test
    public void tableExits() throws IOException {
        System.out.println(TableUtil.tableIsExists(connection,"","hyh"));
    }

    @org.junit.Test
    public void listTables() throws IOException {
        System.out.println(TableUtil.listTables(connection));
    }

    @org.junit.Test
    public void createTable() throws IOException {
        System.out.println(TableUtil.createTable(connection,"","t2","info"));
    }

    @org.junit.Test
    public void dropTable() throws IOException {
        System.out.println(TableUtil.dropTable(connection,"","t1"));
    }

}
