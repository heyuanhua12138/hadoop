package com.hyh.hadoop.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    private String orderId;
    private String pId;
    private int amount;
    private String pName;
    private String tableFlag;

    public TableBean() {
        super();
    }

    public TableBean(String orderId, String pId, int amount, String pName, String tableFlag) {
        this.orderId = orderId;
        this.pId = pId;
        this.amount = amount;
        this.pName = pName;
        this.tableFlag = tableFlag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(tableFlag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readUTF();
        pId = dataInput.readUTF();
        amount = dataInput.readInt();
        pName = dataInput.readUTF();
        tableFlag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return orderId + "\t" + pName + "\t" + amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getTableFlag() {
        return tableFlag;
    }

    public void setTableFlag(String tableFlag) {
        this.tableFlag = tableFlag;
    }
}
