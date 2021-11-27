package com.hyh.hbasemapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 从hdfs中读取数据至Hbase
 */
public class HbaseMrMapper2 extends Mapper<LongWritable, Text, Text, Put> {
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        Put put = new Put(Bytes.toBytes(values[0]));
        text.set(values[0]);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(values[1]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(values[2]));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(values[3]));
        context.write(text, put);
    }
}
