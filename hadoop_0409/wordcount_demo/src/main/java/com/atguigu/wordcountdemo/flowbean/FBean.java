package com.atguigu.wordcountdemo.flowbean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FBean implements WritableComparable<FBean> {
    private Integer upFlow;
    private Integer downFlow;
    private Integer sumFlow;

    public FBean() {
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.downFlow+this.upFlow;
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


    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }

    @Override
    public int compareTo(FBean o) {
        int result;

        // 按照总流量大小，倒序排列
        if (this.sumFlow > o.getSumFlow()) {
            result = -1;
        }else if (this.sumFlow < o.getSumFlow()) {
            result = 1;
        }else {
            result = 0;
        }

        return result;

    }
}
