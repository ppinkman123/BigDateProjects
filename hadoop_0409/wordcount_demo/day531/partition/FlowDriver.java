package com.atguigu.hadoop.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author clh
 * @create 2022-05-30-15:35
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件创建job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.绑定driver
        job.setJarByClass(FlowDriver.class);
        //3.绑定mapper和reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //4.指定mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //5.指定最终的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //指定reduce数量
        job.setNumReduceTasks(41);
        //指定分区类用哪个
        job.setPartitionerClass(ProvincePartitioner.class);
        //6.指定输入类型
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputflow"));
        //7.指定输出类型
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop\\part6"));
        //8.提交运行
        job.waitForCompletion(true);
    }
}
