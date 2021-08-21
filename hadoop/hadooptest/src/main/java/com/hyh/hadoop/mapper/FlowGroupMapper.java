package com.hyh.hadoop.mapper;

import com.hyh.hadoop.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowGroupMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    //7 	13560436666	120.196.100.99		1116		 954			200
    //id	手机号码		网络ip			上行流量  下行流量     网络状态码
    Text t = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values [] = value.toString().split("\t");
        t.set(values[1]);
        FlowBean fb = new FlowBean(Integer.valueOf(values[values.length-3]),Integer.valueOf(values[values.length-2]));
        context.write(fb,t);
    }
}
