package com.hyh.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MutiJobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    FileSplit fileSplit = null;
    String fileName;
    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] values = value.toString().split(" ");
        for (String str:values) {
            text.set(str+"--"+fileName);
            context.write(text,intWritable);
        }
    }
}
