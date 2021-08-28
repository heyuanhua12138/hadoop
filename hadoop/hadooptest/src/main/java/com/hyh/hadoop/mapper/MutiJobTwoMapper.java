package com.hyh.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MutiJobTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    Text textKey = new Text();
    Text textValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("--");
        textKey.set(values[0]);
        textValue.set(values[1]);
        context.write(textKey, textValue);
    }
}
