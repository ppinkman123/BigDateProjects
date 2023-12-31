package com.atguigu.hadoop.mapreduce.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //转化手机号字符串
        String phone = text.toString();
        //手机号的前三位
        String prePhone = phone.substring(0, 3);
        if ("136".equals(prePhone)){
            return 0;
        }else if ("137".equals(prePhone)){
            return 1;
        }else if ("138".equals(prePhone)){
            return 2;
        }else if ("139".equals(prePhone)){
            return 3;
        }else {
            return 4;
        }
    }
}
