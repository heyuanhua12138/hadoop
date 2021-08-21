package com.hyh.hadoop.recordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRedordReader extends RecordReader<Text, BytesWritable> {
    private FileSplit split;
    private Configuration configuration;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private boolean progress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //定义缓存区
        if (progress) {
            FileSystem fs = null;
            FSDataInputStream fsi = null;
            try {
                byte[] content = new byte[(int) split.getLength()];
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);
                fsi = fs.open(path);
                //读取文件内容
                IOUtils.readFully(fsi, content, 0, content.length);
                value.set(content, 0, content.length);
                String fileName = path.toString();
                key.set(fileName);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } finally {
//                fs.close();
//                fsi.close();
                IOUtils.closeStream(fsi);
            }
            progress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
