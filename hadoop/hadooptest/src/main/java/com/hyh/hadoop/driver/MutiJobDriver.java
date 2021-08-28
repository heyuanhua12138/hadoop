package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.MutiJobMapper;
import com.hyh.hadoop.reducer.MutiJobReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MutiJobDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/mutijobinput02");
        Path output = new Path("d:/20210828output02");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(MutiJobDriver.class);
        job.setMapperClass(MutiJobMapper.class);
        job.setReducerClass(MutiJobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
