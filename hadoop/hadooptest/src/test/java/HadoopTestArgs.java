import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopTestArgs {

    @Test
    public void testMkir() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        fs.mkdirs(new Path("/20210731/hyh/javaclient/input2/"));
        fs.close();
    }

    @Test
    public void testCopyFromLocal() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        fs.copyFromLocalFile(new Path("d:/clinet_text.txt"),new Path("/20210731/hyh/javaclient/input/clinet_text.txt"));
        fs.close();
    }

    @Test
    public void testCopyToLocal() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        fs.copyToLocalFile(new Path("/20210731/hyh/javaclient/input/clinet_text.txt"),new Path("d:/tools/clinet_text.txt"));
        fs.close();
    }

    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        fs.delete(new Path("/20210731/hyh/javaclient/input2/"),true);
        fs.close();
    }

    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        fs.rename(new Path("/20210731/hyh/javaclient/input/clinet_text.txt"),new Path("/20210731/hyh/javaclient/input/clinet_text_rename.txt"));
        fs.close();
    }

    @Test
    public void testLsitFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        RemoteIterator<LocatedFileStatus> list = fs.listFiles(new Path("/"),true);
        while (list.hasNext()){
            LocatedFileStatus locatedFileStatus = list.next();
            System.out.println("文件名称======"+locatedFileStatus.getPath().getName());
            System.out.println("文件长度======"+locatedFileStatus.getLen());
            System.out.println("文件权限======"+locatedFileStatus.getPermission());
            System.out.println("文件分组======"+locatedFileStatus.getGroup());
            System.out.println("文件块大小======"+locatedFileStatus.getBlockSize());
            //获取存储的块信息
            BlockLocation blockLocations [] = locatedFileStatus.getBlockLocations();
            for (BlockLocation blockLocation:blockLocations) {
                String [] hosts = blockLocation.getHosts();
                for (String host:hosts) {
                    System.out.println("block所在主机========"+host);
                }
            }
            System.out.println("---------------------华丽的分界线---------------------------");
        }
        fs.close();
    }

    @Test
    public void testLsitStatus() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        FileStatus [] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("文件======="+fileStatus.getPath().getName());
            }else{
                System.out.println("文件夹======="+fileStatus.getPath().getName());
            }
        }
        fs.close();
    }

}
