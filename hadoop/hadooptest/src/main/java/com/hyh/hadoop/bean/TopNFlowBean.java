package com.hyh.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopNFlowBean implements WritableComparable<TopNFlowBean> {
    private String tel;
    private int upFlow;
    private int downFlow;
    private int sumFlow;

    public TopNFlowBean() {
        super();
    }

    public TopNFlowBean(String tel, int upFlow, int downFlow) {
        this.tel = tel;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public String toString() {
        return this.tel + "\t" + this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tel);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tel = dataInput.readUTF();
        upFlow = dataInput.readInt();
        downFlow = dataInput.readInt();
        sumFlow = dataInput.readInt();
    }

    public String getTel() {
        return tel;
    }

    public void setTel(String tel) {
        this.tel = tel;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(int sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(TopNFlowBean o) {
        if(this.sumFlow > o.sumFlow){
            return -1;
        }else if(this.sumFlow < o.sumFlow){
            return 1;
        }else{
            return 0;
        }
    }
}
