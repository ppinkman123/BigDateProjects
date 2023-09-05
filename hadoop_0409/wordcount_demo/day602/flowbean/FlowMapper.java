package com.atguigu.hadoop.mapreduce.flowbean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-05-30-15:35
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    private Text outK;
    private FlowBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outK = new Text();
        outV = new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        //2	13846544121	192.196.100.2			264	0	200
        //1.转化字符串 按照制表符切割
        String line = value.toString();
        String[] phones = line.split("\t");
        //2.获取每一行的手机号 上行和下行
        String phone = phones[1];
        String upFlow = phones[phones.length - 3];
        String downFlow = phones[phones.length - 2];
        //3.封装
        outK.set(phone);
        outV.setUpFlow(Integer.parseInt(upFlow));
        outV.setDownFlow(Integer.parseInt(downFlow));
        outV.setSumFlow();
        //4.写出
        context.write(outK,outV);
    }
}
