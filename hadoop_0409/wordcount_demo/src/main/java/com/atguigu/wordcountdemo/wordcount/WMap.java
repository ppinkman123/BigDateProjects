package com.atguigu.wordcountdemo.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WMap extends Mapper<LongWritable,Text,Text,IntWritable> {
    private Text outK;
    private IntWritable outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new IntWritable(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] words = string.split(" ");
        for (String word : words) {
            outK.set(word);
            context.write(outK,outV);
        }
    }
}
