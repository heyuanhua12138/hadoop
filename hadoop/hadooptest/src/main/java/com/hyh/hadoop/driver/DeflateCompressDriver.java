package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.DeflateCompressMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DeflateCompressDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path input = new Path("d:/mrinput/simpleLogCleaning");
        Path output = new Path("d:/20210825output/deflateCompress");
        Configuration configuration = new Configuration();
        //configuration.set("io.compression.codecs","org.apache.hadoop.io.compress.DefaultCodec");
        //configuration.set("mapreduce.map.output.compress", "true");
        //设置Mapper端开启压缩
        configuration.setBoolean("mapreduce.map.output.compress",true);
        configuration.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(output)) {
            fileSystem.delete(output, true);
        }
        Job job = Job.getInstance(configuration);
        job.setJarByClass(DeflateCompressDriver.class);
        job.setMapperClass(DeflateCompressMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        //设置压缩方式
        FileOutputFormat.setOutputCompressorClass(job,DefaultCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        //设置reducer端开启压缩
        //FileOutputFormat.setCompressOutput(job,true);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


}
