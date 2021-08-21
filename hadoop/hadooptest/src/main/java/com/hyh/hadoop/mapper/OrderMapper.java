package com.hyh.hadoop.mapper;

import com.hyh.hadoop.bean.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    OrderBean orderBean = new OrderBean();
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values[] = value.toString().split("\t");
        orderBean.setOrderId(values[0]);
        orderBean.setPrice(Float.valueOf(values[2]));
        context.write(orderBean, NullWritable.get());
    }
}
