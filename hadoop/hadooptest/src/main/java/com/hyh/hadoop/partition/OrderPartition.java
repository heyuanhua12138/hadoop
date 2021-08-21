package com.hyh.hadoop.partition;

import com.hyh.hadoop.bean.OrderBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean, Text> {
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        if (text.toString().equals("0000001")) {
            return 0;
        } else if (text.toString().equals("0000002")) {
            return 1;
        } else if (text.toString().equals("0000003")) {
            return 2;
        } else {
            return 3;
        }
    }
}
