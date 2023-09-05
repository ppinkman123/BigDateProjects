package com.atguigu.hadoop.mapreduce.comparapart;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author clh
 * @create 2022-05-30-15:35
 */
public class FlowBean implements WritableComparable<FlowBean> {
    //私有的属性
    private Integer upFlow;
    private Integer downFlow;
    private Integer sumFlow;

    public FlowBean() {
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

    public void setSumFlow(Integer sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override  //序列化方法
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override //反序列化方法 读回来属性顺序得和写出去的顺序一致
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readInt();
        downFlow = in.readInt();
        sumFlow = in.readInt();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        //我比谁小 返回-1 正序排
        if (this.sumFlow < o.sumFlow) {
            return 1;
        } else if (this.sumFlow > o.sumFlow) {
            return -1;
        } else {
            if (this.upFlow < o.upFlow) {
                return -1;
            } else if (this.upFlow > o.upFlow) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
