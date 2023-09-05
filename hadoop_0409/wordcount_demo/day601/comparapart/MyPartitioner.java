package com.atguigu.hadoop.mapreduce.comparapart;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author clh
 * @create 2022-06-01-10:17
 */
public class MyPartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        //取到手机号前三位
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);
        int partition=0;
        switch (prePhone){
            case "136":
                partition=0;
                break;
            case "137":
                partition=1;
                break;
            case "138":
                partition=2;
                break;
            case "139":
                partition=3;
                break;
                default:
                    partition=4;
        }
        return partition;
    }
}
