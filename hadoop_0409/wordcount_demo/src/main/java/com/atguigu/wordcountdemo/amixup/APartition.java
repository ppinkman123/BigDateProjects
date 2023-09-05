package com.atguigu.wordcountdemo.amixup;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class APartition extends Partitioner<Text,ABean> {
    @Override
    public int getPartition(Text text, ABean aBean, int numPartitions) {
        String string = text.toString();

        String prv = string.substring(0, 3);
        if("135".equals(prv)){
            return 0;
        }return 1;
    }
}
