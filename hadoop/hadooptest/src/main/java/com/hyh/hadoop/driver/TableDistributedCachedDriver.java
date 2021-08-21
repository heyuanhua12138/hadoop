package com.hyh.hadoop.driver;

import com.hyh.hadoop.bean.TableBean;
import com.hyh.hadoop.mapper.TableDistributedCachedMapper;
import com.hyh.hadoop.mapper.TableMapper;
import com.hyh.hadoop.reducer.TableReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TableDistributedCachedDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Path input = new Path("d:/mrinput/mapjoin");
        Path output = new Path("d:/20210820output/mapjoin");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output, true);
        }
        Job job = Job.getInstance(configuration);
        job.setJarByClass(TableDistributedCachedDriver.class);
        job.setMapperClass(TableDistributedCachedMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //job.setCacheFiles(new URI("file:///e:/input/inputcache/pd.txt"));
        job.addCacheFile(new URI("file:///d:/mrinput/pd.txt"));
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
