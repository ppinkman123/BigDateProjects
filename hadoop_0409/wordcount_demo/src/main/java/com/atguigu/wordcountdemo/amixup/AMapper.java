package com.atguigu.wordcountdemo.amixup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AMapper extends Mapper<LongWritable, Text,Text,ABean> {
    private Text outK;
    private ABean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new ABean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phoneNum = split[1];
        String upFlow = split[split.length-3];
        String downFlow = split[split.length-2];

        outK.set(phoneNum);
        outV.setUpFlow(Integer.parseInt(upFlow));
        outV.setDownFlow(Integer.parseInt(downFlow));
        outV.setSumFlow();
        context.write(outK,outV);
    }
}
