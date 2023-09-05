package com.atguigu.wordcountdemo.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1. 继承mapper类
 * 2. 思考泛型是什么
 *              输入一对泛型
 *                  KEYIN      LongWritbale  偏移量(读到什么位置)
 *                  VALUEIN    Text          读进来的一行数据
 *              输出一对泛型
 *                  KEYOUT    Text           单词
 *                  VALUEOUT  IntWritable     1
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outK ;
    private IntWritable outV ;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK=new Text();
        outV=new IntWritable(1);
    }

    /**
     *
     * @param key  每一行偏移量 目前在wordcount里 无用
     * @param value 每一行数据  需要用
     * @param context 上下文件对象 主要作用是将数据结果发送给reducer
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // atguigu atguigu
        // 需要先转换成字符串 数据切分
        String line = value.toString();
        String[] words = line.split(" ");
        //[atguigu,atguigu]
        //遍历集合  通过上下文件对象 挨个打上1后写出
        for (String word : words) {
            outK.set(word);
            context.write(outK,outV);
        }
    }
}
