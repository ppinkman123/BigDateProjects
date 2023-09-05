package com.atguigu.wordcountdemo.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCount extends Reducer<Text, IntWritable,Text, IntWritable> {
    private IntWritable outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new IntWritable(0);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
        for (IntWritable value : values) {
            sum+=value.get();
        }
            outV.set(sum);

        context.write(key,outV);
    }
}
