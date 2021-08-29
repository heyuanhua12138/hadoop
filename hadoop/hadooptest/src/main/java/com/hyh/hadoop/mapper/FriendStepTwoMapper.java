package com.hyh.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    //输入
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
    //期望输出
    //A-B	E C
    //A-C	D F
    //A-D	E F
    //A-E	D B C
    //A-F	O B C D E
    //A-G	F E C D
    //A-H	E C D O
    //A-I	O
    //A-J	O B
    //A-K	D C
    //...
    private Text person = new Text();
    private Text friend = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        person.set(values[0]);
        String[] friends = values[1].split(",");
        Arrays.sort(friends);
        for (int i = 0; i <friends.length-1 ; i++) {
            for (int j = i+1; j <friends.length ; j++) {
                friend.set(friends[i]+"-"+friends[j]);
                context.write(friend,person);
            }
        }
    }
}
