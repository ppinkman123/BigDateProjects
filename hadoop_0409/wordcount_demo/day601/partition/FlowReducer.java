package com.atguigu.hadoop.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-05-30-15:35
 */
public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    private FlowBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      outV = new FlowBean();
    }

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //13568436656	[flowbean(2481,24681,xxxxx) 	flowbean(1116,954,xxxxx)]
        //定义两个求和变量
        int totalUpFlow=0;
        int totalDownFlow=0;
        //迭代集合
        for (FlowBean value : values) {
            //求和
            totalUpFlow+=value.getUpFlow();
            totalDownFlow+=value.getDownFlow();
        }
        //封装写出
        outV.setUpFlow(totalUpFlow);
        outV.setDownFlow(totalDownFlow);
        outV.setSumFlow();
        context.write(key,outV);
    }
}
