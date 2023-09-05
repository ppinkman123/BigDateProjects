package com.atguigu.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.PrintStream;

/**
 * @author clh
 * @create 2022-06-02-9:23
 */
public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {

    private String fileName;
    private Text outK=new Text();
    private TableBean outV=new TableBean();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //mysql join的时候怎么写的
        //from xxxx join xxxx
        //表名是确定的  拿到这行数据对应表名是什么
        //通过上下文对象拿到对应map正在处理的切片名称
        //java中强转的前提是什么 继承 本身你得是那个子类
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //先转化字符串 通过文件的格式或者内容来将数据区分开 本身就不对的
        String line = value.toString();
        if (fileName.contains("order")){
            String[] orderInfo = line.split("\t");
            String id = orderInfo[0];
            String pid = orderInfo[1];
            String amount = orderInfo[2];
            outK.set(pid);
            outV.setId(id);
            outV.setPid(pid);
            outV.setAmount(Integer.parseInt(amount));
            outV.setPname("");
            outV.setFlag("order");
        }else{
            String[] pdInfo = line.split(" ");
            String pid = pdInfo[0];
            String pname = pdInfo[1];
            outK.set(pid);
            outV.setId("");
            outV.setPid(pid);
            outV.setAmount(0);
            outV.setPname(pname);
            outV.setFlag("pd");
        }
        context.write(outK,outV);
    }
}
