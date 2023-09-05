package com.atguigu.wordcountdemo.flowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FMap extends Mapper<LongWritable, Text,Text,FBean> {
    private Text outK;
    private FBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new FBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] phone = value.toString().split("\t");
        String up = phone[phone.length - 3];

        String down = phone[phone.length - 2];

        outK.set(phone[1]);

        outV.setUpFlow(Integer.parseInt(up));
        outV.setDownFlow(Integer.parseInt(down));
        outV.setSumFlow();

        context.write(outK,outV);
    }

}
