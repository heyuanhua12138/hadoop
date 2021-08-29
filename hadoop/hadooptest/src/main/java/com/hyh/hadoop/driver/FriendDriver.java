package com.hyh.hadoop.driver;

import com.hyh.hadoop.mapper.FriendStepOneMapper;
import com.hyh.hadoop.mapper.FriendStepTwoMapper;
import com.hyh.hadoop.reducer.FriendStepOneReducer;
import com.hyh.hadoop.reducer.FriendStepTwoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Path stepOneInput = new Path("d:/mrinput/friend");
        Path stepOneOutput = new Path("d:/20210829output/friendStep1");
        Path stepTwoOutput = new Path("d:/20210829output/friendFinal");
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.isDirectory(stepOneOutput)) {
            fileSystem.delete(stepOneOutput, true);
        }
        Job stepOneJob = Job.getInstance();
        stepOneJob.setJarByClass(FriendDriver.class);
        stepOneJob.setMapperClass(FriendStepOneMapper.class);
        stepOneJob.setReducerClass(FriendStepOneReducer.class);
        stepOneJob.setMapOutputKeyClass(Text.class);
        stepOneJob.setMapOutputValueClass(Text.class);
        stepOneJob.setOutputKeyClass(Text.class);
        stepOneJob.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(stepOneJob, stepOneInput);
        FileOutputFormat.setOutputPath(stepOneJob, stepOneOutput);
        boolean stepOneResult = stepOneJob.waitForCompletion(true);
        if (stepOneResult) {
            if (fileSystem.isDirectory(stepTwoOutput)) {
                fileSystem.delete(stepTwoOutput, true);
            }
            Job stepTwoJob = Job.getInstance();
            stepTwoJob.setJarByClass(FriendDriver.class);
            stepTwoJob.setMapperClass(FriendStepTwoMapper.class);
            stepTwoJob.setReducerClass(FriendStepTwoReducer.class);
            stepTwoJob.setMapOutputKeyClass(Text.class);
            stepTwoJob.setMapOutputValueClass(Text.class);
            stepTwoJob.setOutputKeyClass(Text.class);
            stepTwoJob.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(stepTwoJob, stepOneOutput);
            FileOutputFormat.setOutputPath(stepTwoJob, stepTwoOutput);
            boolean result = stepTwoJob.waitForCompletion(true);
            System.exit(result ? 0 : 1);
        }
    }
}
