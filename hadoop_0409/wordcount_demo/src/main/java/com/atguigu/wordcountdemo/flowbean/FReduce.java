package com.atguigu.wordcountdemo.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FReduce extends Reducer<Text, FBean, Text, FBean> {

    private FBean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new FBean();
    }

    @Override
    protected void reduce(Text key, Iterable<FBean> values, Context context) throws IOException, InterruptedException {
        int Up = 0;
        int Down = 0;

        for (FBean value : values) {
            Up += value.getUpFlow();
            Down += value.getDownFlow();

        }
        outV.setUpFlow(Up);
        outV.setDownFlow(Down);
        outV.setSumFlow();

        context.write(key, outV);

    }
}
