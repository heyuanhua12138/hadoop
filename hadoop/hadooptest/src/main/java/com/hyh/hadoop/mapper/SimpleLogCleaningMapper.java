package com.hyh.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SimpleLogCleaningMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean cleanResult = CleanResult(line, context);
        if (!cleanResult) {
            return;
        }
        text.set(value);
        context.write(text, NullWritable.get());
    }

    private boolean CleanResult(String line, Context context) {
        String [] lines = line.split(" ");
        if (lines.length > 11) {
            context.getCounter("cleanmap", "true").increment(1);
            return true;
        } else {
            context.getCounter("cleanmap", "false").increment(1);
            return false;
        }
    }
}
