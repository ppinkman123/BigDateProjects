package com.atguigu.hadoop.mapreduce.testshuffle;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-06-01-11:22
 */
public class LogMapper extends Mapper<LongWritable, Text,NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(),value);
    }
}
