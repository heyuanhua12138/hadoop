package com.hyh.hbasemapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HbaseMrDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HbaseMrDriver.class);
        TableMapReduceUtil.initTableMapperJob("t1", new Scan(), HbaseMrMapper.class, Text.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("t2", HbaseMrReducer.class, job);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
