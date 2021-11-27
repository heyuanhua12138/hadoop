package com.hyh.hbasemapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class HbaseMrDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HbaseMrDriver2.class);
        job.setMapperClass(HbaseMrMapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Put.class);
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop103:9000/tmp_hbase"));
        TableMapReduceUtil.initTableReducerJob("t4", HbaseMrReducer2.class, job);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
