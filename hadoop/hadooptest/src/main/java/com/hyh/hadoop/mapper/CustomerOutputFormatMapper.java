package com.hyh.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CustomerOutputFormatMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        text.set(value.toString());
        context.write(text,NullWritable.get());
    }



}
