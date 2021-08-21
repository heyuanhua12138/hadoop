package com.hyh.hadoop.mapper;

import com.hyh.hadoop.bean.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private FileSplit fileSplit = null;
    String fileName;
    TableBean tableBean = new TableBean();
    Text text = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //订单
        //1001	01	1
        //1002	02	2
        //1003	03	3
        //1004	01	4
        //1005	02	5
        //1006	03	6
        //商品
        //01	小米
        //02	华为
        //03	格力
        String values[] = value.toString().split("\t");
        if (fileName.indexOf("order") != -1) {
            //订单
            tableBean.setOrderId(values[0]);
            tableBean.setpId(values[1]);
            tableBean.setAmount(Integer.valueOf(values[2]));
            tableBean.setTableFlag("order");
            tableBean.setpName("");
            text.set(tableBean.getpId());
            context.write(text, tableBean);
        } else {
            tableBean.setOrderId("");
            tableBean.setAmount(0);
            tableBean.setpId(values[0]);
            tableBean.setpName(values[1]);
            tableBean.setTableFlag("product");
            text.set(tableBean.getpId());
            context.write(text, tableBean);
        }

    }
}
