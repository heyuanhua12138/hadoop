package com.hyh.hadoop.mapper;

import com.hyh.hadoop.bean.TopNFlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class TopNMapper extends Mapper<LongWritable, Text, TopNFlowBean, NullWritable> {
    //Map<TopNFlowBean,Object> treeMap = new TreeMap();
    TreeMap<TopNFlowBean,String> treeMap = new TreeMap();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values[] = value.toString().split("\t");
        TopNFlowBean topNFlowBean = new TopNFlowBean(values[0],Integer.valueOf(values[1]),Integer.valueOf(values[2]));
        treeMap.put(topNFlowBean,values[0]);
        if(treeMap.size()>10){
            //treeMap = (TreeMap<TopNFlowBean, Object>) treeMap.remove(treeMap.lastKey());
            treeMap.remove(treeMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<TopNFlowBean> iterator = treeMap.keySet().iterator();
        while (iterator.hasNext()){
            context.write(iterator.next(),NullWritable.get());
        }
//        for (Iterator<TopNFlowBean> it = treeMap.keySet().iterator(); it.hasNext(); ) {
//            TopNFlowBean topNFlowBean = it.next();
//        }
    }
}
