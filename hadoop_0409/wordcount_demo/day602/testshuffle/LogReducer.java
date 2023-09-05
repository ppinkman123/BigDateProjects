package com.atguigu.hadoop.mapreduce.testshuffle;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-06-01-11:22
 */
public class LogReducer extends Reducer<NullWritable, Text,Text,NullWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //迭代写出
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
