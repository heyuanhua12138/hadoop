package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.MutiJobTwoMapper;
import com.hyh.hadoop.reducer.MutiJobTwoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MutiJobTwoDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/mutijobinput/two");
        Path output = new Path("d:/20210828output/two");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(MutiJobTwoDriver.class);
        job.setMapperClass(MutiJobTwoMapper.class);
        job.setReducerClass(MutiJobTwoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
