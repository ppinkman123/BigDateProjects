package com.atguigu.wordcountdemo.flowbean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FDriver.class);
        //3.绑定mapper和reducer
        job.setMapperClass(FMap.class);
        job.setReducerClass(FReduce.class);
        //4.指定mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FBean.class);
        //5.指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FBean.class);
        //指定分区数量
//        job.setNumReduceTasks(4);
//        自定义分区
//        job.setPartitionerClass(Fpart.class);
        //6.指定输入类型
        FileInputFormat.setInputPaths(job,new Path("D:\\大数据资料\\maven\\day1\\linux_hadoop\\05_尚硅谷大数据技术之Hadoop\\2.资料\\09_测试数据\\input\\inputflow\\phone_data.txt"));
        //7.指定输出类型
        FileOutputFormat.setOutputPath(job,new Path("D:/tool/com2"));
        //8.提交运行
        job.waitForCompletion(true);
    }
}
