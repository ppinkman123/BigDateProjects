package com.atguigu.wordcountdemo.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1.继承reduce类
 * 2.思考泛型
 *        输入一对
 *             KEYIN     Text          单词
 *             VALUEIN   IntWritable   1
 *        输出一对
 *             KEYOUT    Text          单词
 *             VALUEOUT  Intwritable   总次数
 *
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

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
