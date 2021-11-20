import com.hyh.hbase.util.HbaseConnection;
import com.hyh.hbase.util.NameSpaceUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

public class Test {
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
    public void listNameSpaces() throws IOException {
        List<String> list = NameSpaceUtil.ListNameSpaces(connection);
        System.out.println(list);
    }

    @org.junit.Test
    public void testExistsNameSpace() throws IOException {
        System.out.println(NameSpaceUtil.existsNameSpace(connection,"hbase"));
    }

    @org.junit.Test
    public void testCreateNameSpace() throws IOException {
        NameSpaceUtil.createNameSpace(connection,"hyh");
    }

    @org.junit.Test
    public void testDeleteNameSpace() throws IOException {
        NameSpaceUtil.deleteNameSpace(connection,"hyh");
    }
}
