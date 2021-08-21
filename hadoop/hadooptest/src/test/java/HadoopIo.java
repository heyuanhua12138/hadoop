import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopIo {
    @Test
    public void testUpload() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        FileInputStream fileInputStream = new FileInputStream(new File("d:/clinet_text.txt"));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/user/clinet_text.txt"));
        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,configuration);
        fileInputStream.close();
        fsDataOutputStream.close();
        fs.close();
    }

    @Test
    public void testDownload() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        FSDataInputStream fsDataInputStream = fs.open(new Path("/user/hyh/input/profile"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("d:/profile"));
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,configuration);
        fsDataInputStream.close();
        fileOutputStream.close();
        fs.close();
    }
}
