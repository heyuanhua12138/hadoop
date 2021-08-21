package com.hyh.hadoop.grouping;

import com.hyh.hadoop.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingCompartor extends WritableComparator {

    protected OrderGroupingCompartor() {
        super(OrderBean.class,true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean ob1 = (OrderBean) a;
        OrderBean ob2 = (OrderBean) b;

        if(Integer.valueOf(ob1.getOrderId())>Integer.valueOf(ob2.getOrderId())){
            return 1;
        }else if(Integer.valueOf(ob1.getOrderId())<Integer.valueOf(ob2.getOrderId())){
            return -1;
        }else{
            //return  ob1.getPrice()>ob2.getPrice() ? -1:1;
            return  0;
        }
    }
}
