package com.hyh.hadoop.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TableDistributedCachedMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> cachMap = new HashMap<>();
    private String orderId;
    private int amount;
    private String pId;
    private String pName;
    private Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        File file = new File("pd.txt","UTF-8");
//        FileInputStream fileInputStream = new FileInputStream(file);
//        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
//        BufferedReader br = new BufferedReader(inputStreamReader);
        //BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pd.txt"),"UTF-8"));
        URI[] files = context.getCacheFiles();
        for (URI file : files) {
            BufferedReader br = new BufferedReader(new FileReader(new File(file)));
            String line;
            while (StringUtils.isNotEmpty(line = br.readLine())) {
                String lines[] = line.split("\t");
                cachMap.put(lines[0], lines[1]);
            }
            br.close();
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values[] = value.toString().split("\t");
        orderId = values[0];
        pName = cachMap.get(values[1]);
        amount = Integer.valueOf(values[2]);
        String str = orderId + "\t" + pName + "\t" + amount;
        text.set(str);
        context.write(text, NullWritable.get());
    }
}
