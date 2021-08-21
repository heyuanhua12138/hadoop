package com.hyh.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //    int sum;
//    IntWritable v = new IntWritable();
//    @Override
//    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
//        // 1 累加求和
//		sum = 0;
//        Iterable<IntWritable> values2 = values;
//		for (IntWritable count : values2) {
//			sum += count.get();
//		}
//
//		// 2 输出
//       v.set(sum);
//		context.write(key,v);
//    }
    int sum;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}


//int sum;
//IntWritable v = new IntWritable();
//
//	@Override
//	protected void reduce(Text key, Iterable<IntWritable> values,
//			Context context) throws IOException, InterruptedException {
//
//		// 1 累加求和
//		sum = 0;
//		for (IntWritable count : values) {
//			sum += count.get();
//		}
//
//		// 2 输出
//       v.set(sum);
//		context.write(key,v);
//	}
//}