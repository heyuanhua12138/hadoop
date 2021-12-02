import com.hyh.hbase.util.DataUtil;
import com.hyh.hbase.util.HbaseConnection;
import com.hyh.hbase.util.TableUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class TestData {
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
    public void tablePut() throws IOException {
        DataUtil.put(connection,"","t2","a3","info","name","hyh2");
    }

    @org.junit.Test
    public void tableGet() throws IOException {
        DataUtil.get(connection,"","t2","a2");
    }

    @org.junit.Test
    public void tableScan() throws IOException {
        DataUtil.scan(connection,"","t2");
    }

    @org.junit.Test
    public void tableDelete() throws IOException {
        DataUtil.delete(connection,"","t2","a3");
    }

    @org.junit.Test
    public void putWithSetAutoFlushTo() throws IOException {
        DataUtil.putWithSetAutoFlushTo(connection,"","t2","a5","info","name","hyh5");
    }


}
