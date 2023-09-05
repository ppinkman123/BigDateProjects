package com.atguigu.hadoop.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-06-01-10:30
 */
public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    private IntWritable outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new IntWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //第一组 atguigu  [1,1]
        //定义一个求和变量
        int sum=0;
        //遍历 values
        for (IntWritable value : values) {
            sum+=value.get();
        }
        //封装
        outV.set(sum);
        //将结果写出并落盘
        context.write(key, outV);
    }
}
