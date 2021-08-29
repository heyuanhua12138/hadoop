package com.hyh.hadoop.driver;

import com.hyh.hadoop.bean.TopNFlowBean;
import com.hyh.hadoop.mapper.TopNMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopNDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/topN");
        Path output = new Path("d:/20210829output/topN");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(TopNDriver.class);
        job.setMapperClass(TopNMapper.class);
        job.setOutputKeyClass(TopNFlowBean.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
