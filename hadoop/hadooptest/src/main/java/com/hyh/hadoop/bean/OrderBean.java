package com.hyh.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private float price;

    public OrderBean() {

    }

    public OrderBean(String orderId, float price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        if(Integer.valueOf(this.orderId)>Integer.valueOf(o.getOrderId())){
            return 1;
        }else if(Integer.valueOf(this.orderId)<Integer.valueOf(o.getOrderId())){
            return -1;
        }else{
            return this.price > o.price ? -1 : 1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeFloat(this.price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        price = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }
}
