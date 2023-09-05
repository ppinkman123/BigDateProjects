package com.atguigu.hadoop.mapreduce.comparable;

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
        //进行数据转化
        String line = value.toString();
        String[] phones = line.split("\t");
        String phone = phones[0];
        String upFlow = phones[1];
        String downFlow = phones[2];
        //封装写出
        outK.setUpFlow(Integer.parseInt(upFlow));
        outK.setDownFlow(Integer.parseInt(downFlow));
        outK.setSumFlow();
        outV.set(phone);
        context.write(outK,outV);
    }
}
