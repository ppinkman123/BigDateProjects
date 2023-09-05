package com.atguigu.wordcountdemo.amixup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.checkerframework.checker.units.qual.K;

import java.io.IOException;

public class AReducer extends Reducer<Text,ABean,Text,ABean> {
    private ABean outV;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outV = new ABean();
    }

    @Override
    protected void reduce(Text key, Iterable<ABean> values, Context context) throws IOException, InterruptedException {
        for (ABean value : values) {
            context.write(key,value);
        }
    }


}
