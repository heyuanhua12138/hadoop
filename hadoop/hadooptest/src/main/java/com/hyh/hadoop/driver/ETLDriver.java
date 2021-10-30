package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.ETLMapper;
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

public class ETLDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
//        Path input = new Path("d:/mrinput/0223");
//        Path output = new Path("d:/20210920output");
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Configuration configuration = new Configuration();
        //FileSystem fileSystem = FileSystem.get(configuration);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop103:9000"),configuration,"hyh");
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output, true);
        }
        //Job job = Job.getInstance();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean reuslt = job.waitForCompletion(true);
        System.exit(reuslt ? 0 : 1);
    }
}
