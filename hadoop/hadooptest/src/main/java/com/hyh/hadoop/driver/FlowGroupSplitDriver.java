package com.hyh.hadoop.driver;

import com.hyh.hadoop.bean.FlowBean;
import com.hyh.hadoop.mapper.FlowGroupMapper;
import com.hyh.hadoop.partition.GroupFlowPartition;
import com.hyh.hadoop.reducer.FlowGroupReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowGroupSplitDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/flowinput/one");
        Path output = new Path("d:/20210814output/flowoutputsplitgroup5");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.isDirectory(output)) {
            fs.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        job.setJarByClass(FlowGroupSplitDriver.class);
        job.setMapperClass(FlowGroupMapper.class);
        job.setReducerClass(FlowGroupReducer.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setPartitionerClass(GroupFlowPartition.class);
        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
