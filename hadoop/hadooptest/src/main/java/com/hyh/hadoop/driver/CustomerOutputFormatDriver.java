package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.CustomerOutputFormatMapper;
import com.hyh.hadoop.outputFormat.CustomerOutputFormat;
import com.hyh.hadoop.reducer.CustomerOutputFormatReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomerOutputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/outputformat");
        Path output = new Path("d:/mrinput/20210817output");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output,true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(CustomerOutputFormatDriver.class);
        job.setMapperClass(CustomerOutputFormatMapper.class);
        job.setReducerClass(CustomerOutputFormatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(CustomerOutputFormat.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job,output);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
