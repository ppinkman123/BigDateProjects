package com.atguigu.wordcountdemo.flowbean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Fpart extends Partitioner<Text,FBean> {
    @Override
    public int getPartition(Text text, FBean fBean, int numPartitions) {
        String phone = text.toString();
        String cod3 = phone.substring(0, 3);

        if("136".equals(cod3)){
            return 0;
        }else if("137".equals(cod3)){
            return 1;
        }else if("138".equals(cod3)){
            return 2;
        }else {
            return 3;
        }
    }
}
