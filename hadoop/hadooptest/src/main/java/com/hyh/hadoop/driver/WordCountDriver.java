package com.hyh.hadoop.driver;

import java.io.IOException;
import java.net.URISyntaxException;

import com.hyh.hadoop.mapper.WordCountMapper;
import com.hyh.hadoop.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDriver {
    private Path output;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
//        Path input = new Path("/20210801/input");
//        Path output = new Path("/20210801/output");
        Path input = new Path("d:/input");
        Path output = new Path("d:/20210807/output");
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        //configuration.set("fs.defaultFS","hdfs://hadoop103:9000");
        FileSystem fs = FileSystem.get(configuration);
        if (fs.isDirectory(output)) {
            fs.delete(output, true);
        }
        Job job = Job.getInstance();
        //job.setInputFormatClass(CombineFileInputFormat.class);
        //CombineFileInputFormat.setMaxInputSplitSize(job,4194304);

        // 2 设置jar加载路径
        job.setJarByClass(WordCountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}


//// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
//args = new String[] { "e:/input/inputword", "e:/output1" };
//
//		// 1 获取配置信息以及封装任务
//		Configuration configuration = new Configuration();
//		Job job = Job.getInstance(configuration);
//
//		// 2 设置jar加载路径
//		job.setJarByClass(WordcountDriver.class);
//
//		// 3 设置map和reduce类
//		job.setMapperClass(WordcountMapper.class);
//		job.setReducerClass(WordcountReducer.class);
//
//		// 4 设置map输出
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
//
//		// 5 设置Reduce输出
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//
//		// 6 设置输入和输出路径
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//		// 7 提交
//		boolean result = job.waitForCompletion(true);
//
//		System.exit(result ? 0 : 1);
//	}