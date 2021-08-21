package com.hyh.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyValueMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text t = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //banzhang ni hao
        //xihuan hadoop banzhang
        //banzhang ni hao
        //xihuan hadoop banzhang
        //统计第一个单词出现的次数
        String str = value.toString().split(" ")[0];
        t.set(str);
        context.write(t, new IntWritable(1));
    }
}
