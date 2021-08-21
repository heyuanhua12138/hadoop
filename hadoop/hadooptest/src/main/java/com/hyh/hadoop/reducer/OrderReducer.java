package com.hyh.hadoop.reducer;

import com.hyh.hadoop.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, Text,OrderBean, NullWritable> {
    Text text = new Text();
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
