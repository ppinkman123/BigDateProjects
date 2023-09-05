package com.atguigu.wordcountdemo.amixup;

import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.common.value.qual.StaticallyExecutable;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ABean implements WritableComparable<ABean> {
    //手机号的bean
    private int upFlow;
    private int downFlow;
    private int sumFlow;

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.downFlow+this.upFlow;
    }

    public ABean() {
    }

    @Test
    public void o(){
        System.out.println(this.downFlow!=this.getDownFlow());
    }


    @Override
    public String toString() {
        return this.upFlow+"\t"+this.downFlow+"\t"+this.sumFlow;
    }

    @Override
    public int compareTo(ABean o) {
        if (this.getSumFlow() > o.getSumFlow()) {
            return -1;
        } else if (this.getSumFlow() < o.getSumFlow()) {
            return 1;
        }else {
            if (this.getUpFlow() > o.getUpFlow()) {
                return -1;
            } else if (this.getUpFlow() < o.getUpFlow()) {
                return 1;
            } else return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readInt();
        downFlow = in.readInt();
        sumFlow = in.readInt();
    }
}
