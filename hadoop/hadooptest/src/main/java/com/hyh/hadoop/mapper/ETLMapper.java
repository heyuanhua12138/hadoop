package com.hyh.hadoop.mapper;

import com.hyh.hadoop.util.ETLUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String oriStr = value.toString();
        StringBuffer stringBuffer = ETLUtils.vedioETL(oriStr);
//        if(StringUtils.isEmpty(stringBuffer.toString())){
//            return;
//        }
        if (stringBuffer == null) {
            return;
        }
        text.set(stringBuffer.toString());
        context.write(text, NullWritable.get());
    }
}
