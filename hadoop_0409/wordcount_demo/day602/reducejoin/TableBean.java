package com.atguigu.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author clh
 * @create 2022-06-02-9:23
 */
public class TableBean implements Writable {
    //私有的属性
    private String id;
    private String pid;
    private Integer amount;
    private String pname;
    //加个标记属性
    private String flag;

    public TableBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override//序列化
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override//反序列化
    public void readFields(DataInput in) throws IOException {
        id=in.readUTF();
        pid=in.readUTF();
        amount=in.readInt();
        pname=in.readUTF();
        flag=in.readUTF();
    }

    @Override
    public String toString() {
        return id+"\t"+pname+"\t"+amount;
    }
}
