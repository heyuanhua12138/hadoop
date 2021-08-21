package com.hyh.hadoop.reducer;

import com.hyh.hadoop.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text,FlowBean> {
    FlowBean fb = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow=0;
        int downFlow=0;
        int sumFlow=0;
        for (FlowBean flowBean:values) {
            upFlow+=flowBean.getUpFlow();
            downFlow+=flowBean.getDownFlow();
            sumFlow+=flowBean.getSumFlow();
        }
        fb.setUpFlow(upFlow);
        fb.setDownFlow(downFlow);
        fb.setSumFlow(sumFlow);
        context.write(key,fb);
    }
}
