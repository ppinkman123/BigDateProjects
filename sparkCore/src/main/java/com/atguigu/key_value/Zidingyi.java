package com.atguigu.key_value;

import org.apache.spark.Partitioner;

public class Zidingyi extends Partitioner {
    Integer num = 0;

    public Zidingyi(int i){
        num = i;
    }
    @Override
    public int numPartitions() {
        return num;
    }

    @Override
    public int getPartition(Object key) {
        if(key instanceof Integer){
            return (Integer) key% num;
        }
        return 0;
    }
}
