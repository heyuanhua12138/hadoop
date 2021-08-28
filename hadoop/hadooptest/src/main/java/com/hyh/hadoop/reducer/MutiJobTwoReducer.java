package com.hyh.hadoop.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MutiJobTwoReducer extends Reducer<Text,Text,Text, Text> {
    //输入
    //atguigu--a.txt	3
    //atguigu--b.txt	2
    //atguigu--c.txt	2
    //输出atguigu	c.txt-->2	b.txt-->2	a.txt-->3
    Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        String str;
        String [] strArr={};
        for (Text text:values) {
            str=text.toString();
            strArr= str.split("\t");
            sb.append(strArr[0]+"-->"+strArr[1]+"\t");
        }
        text.set(sb.toString());
        context.write(key,text);
    }
}
