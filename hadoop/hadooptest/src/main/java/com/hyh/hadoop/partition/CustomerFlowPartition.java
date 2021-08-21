package com.hyh.hadoop.partition;

import com.hyh.hadoop.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomerFlowPartition extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        //手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中
        if (text.toString().indexOf("136") > -1) {
            return 0;
        } else if (text.toString().indexOf("137") > -1) {
            return 1;
        } else if (text.toString().indexOf("138") > -1) {
            return 2;
        } else if (text.toString().indexOf("139") > -1) {
            return 3;
        } else {
            return 4;
        }

    }
}
