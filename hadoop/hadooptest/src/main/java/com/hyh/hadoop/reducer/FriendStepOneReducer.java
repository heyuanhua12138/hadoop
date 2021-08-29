package com.hyh.hadoop.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendStepOneReducer extends Reducer<Text,Text,Text,Text> {
    Text t = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer();
        for (Text text:values) {
            stringBuffer.append(text+",");
        }
        t.set(stringBuffer.toString());
        context.write(key,t);
    }
}
