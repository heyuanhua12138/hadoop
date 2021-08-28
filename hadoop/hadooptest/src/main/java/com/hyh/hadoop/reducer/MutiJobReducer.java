package com.hyh.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MutiJobReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //values.iterator().hasNext()
        sum = 0;
        for (IntWritable intWritable : values) {
            sum++;
        }
        context.write(key, new IntWritable(sum));
    }
}
