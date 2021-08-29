package com.hyh.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {
    //输入
    //A:B,C,D,F,E,O
    //B:A,C,E,K
    //C:F,A,D,I
    //D:A,E,F,L
    //E:B,C,D,M,L
    //F:A,B,C,D,E,O,M
    //G:A,C,D,E,F
    //H:A,C,D,E,O
    //I:A,O
    //J:B,O
    //K:A,C,D
    //L:D,E,F
    //M:E,F,G
    //O:A,H,I,J
    //期望输出
    //A	I,K,C,B,G,F,H,O,D,
    //B	A,F,J,E,
    //C	A,E,B,H,F,G,K,
    //D	G,C,K,A,L,F,E,H,
    //E	G,M,L,H,A,F,B,D,
    //F	L,M,D,C,G,A,
    //G	M,
    //H	O,
    //I	O,C,
    //J	O,
    //K	B,
    //L	D,E,
    //M	E,F,
    //O	A,H,I,J,F,
    private Text person = new Text();
    private Text friend = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(":");
        person.set(values[0]);
        String[] friends = values[1].split(",");
        for (String string : friends) {
            friend.set(string);
            context.write(friend, person);
        }
    }
}
