package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.MutiJobMapper;
import com.hyh.hadoop.mapper.MutiJobTwoMapper;
import com.hyh.hadoop.reducer.MutiJobReducer;
import com.hyh.hadoop.reducer.MutiJobTwoReducer;
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

public class MutiJobFinalDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/mutijobinput02");
        Path firstOutput = new Path("d:/20210828output03/first");
        Path finalOutput = new Path("d:/20210828output03/final");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(firstOutput)) {
            fileSystem.delete(firstOutput, true);
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
        FileOutputFormat.setOutputPath(job, firstOutput);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        boolean result = job.waitForCompletion(true);
        if (result) {
            if (fileSystem.isDirectory(finalOutput)) {
                fileSystem.delete(finalOutput, true);
            }
            Job secondJob = Job.getInstance();
            secondJob.setJarByClass(MutiJobFinalDriver.class);
            secondJob.setMapperClass(MutiJobTwoMapper.class);
            secondJob.setReducerClass(MutiJobTwoReducer.class);
            secondJob.setMapOutputKeyClass(Text.class);
            secondJob.setMapOutputValueClass(Text.class);
            secondJob.setOutputKeyClass(Text.class);
            secondJob.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(secondJob, firstOutput);
            FileOutputFormat.setOutputPath(secondJob, finalOutput);
            boolean resultSecJob = secondJob.waitForCompletion(true);
            System.exit(resultSecJob ? 0 : 1);
        }
    }
}
