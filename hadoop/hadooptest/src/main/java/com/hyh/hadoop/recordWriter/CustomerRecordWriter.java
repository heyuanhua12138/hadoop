package com.hyh.hadoop.recordWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CustomerRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fsDataOutputStreamhyh = null;
    FSDataOutputStream fsDataOutputStreamOther = null;

    public CustomerRecordWriter(TaskAttemptContext taskAttemptContext) {

        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            fsDataOutputStreamhyh = fs.create(new Path("d:/20210817output/hyh.log"));
            fsDataOutputStreamOther = fs.create(new Path("d:/20210817output/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if(text.toString().indexOf("hyh")>-1){
            fsDataOutputStreamhyh.write(text.toString().getBytes());
        }else{
            fsDataOutputStreamOther.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fsDataOutputStreamhyh.close();
        fsDataOutputStreamOther.close();
    }
}
