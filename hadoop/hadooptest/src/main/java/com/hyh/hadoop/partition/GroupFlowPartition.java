package com.hyh.hadoop.partition;

import com.hyh.hadoop.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupFlowPartition extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
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
