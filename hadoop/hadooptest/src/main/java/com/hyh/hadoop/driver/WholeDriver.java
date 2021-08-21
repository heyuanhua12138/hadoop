package com.hyh.hadoop.driver;

import com.hyh.hadoop.inputFormat.WholeInputFormat;
import com.hyh.hadoop.mapper.WholeMapper;
import com.hyh.hadoop.reducer.WholeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class WholeDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/custom");
        Path output = new Path("d:/20210814output/custom2");
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        if (fs.isDirectory(output)) {
            fs.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(WholeDriver.class);
        job.setMapperClass(WholeMapper.class);
        job.setReducerClass(WholeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(WholeInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //WholeInputFormat.setInputPaths(job, input);
        //SequenceFileOutputFormat.setOutputPath(job, output);
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
