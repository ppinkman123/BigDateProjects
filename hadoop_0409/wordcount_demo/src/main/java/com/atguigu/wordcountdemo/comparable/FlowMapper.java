package com.atguigu.wordcountdemo.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-05-30-15:35
 */
public class FlowMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    private FlowBean outK;
    private Text outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new FlowBean();
        outV = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String phone = value.toString();
        String[] phones = phone.split("\t");
        String num = phones[0];
        int up = Integer.parseInt(phones[1]);
        int down = Integer.parseInt(phones[2]);

        outK.setDownFlow(down);
        outK.setUpFlow(up);
        outK.setSumFlow();
        outV.set(num);
        context.write(outK,outV);
    }
}
