package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.LogMapper;
import com.hyh.hadoop.mapper.SimpleLogCleaningMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/simpleLogCleaning");
        Path output = new Path("d:/20210821output/complexLog");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
