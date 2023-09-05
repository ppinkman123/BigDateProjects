package com.atguigu.hadoop.mapreduce.comparapart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-05-30-15:35
 */
public class FlowReducer extends Reducer<FlowBean, Text,Text, FlowBean> {
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           //防止总流量相同，迭代
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
