package com.atguigu.wordcountdemo.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于客户端 发送程序执行
 *     本地  我们目前是本地
 *     yarn
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件，来获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.绑定driver(本地) 或者 绑定jar包(yarn)
        job.setJarByClass(WordCountDriver.class);
        //3.绑定mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4.指定Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.指定最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置combiner
        job.setCombinerClass(WordCountReducer.class);
        //需要reduce嘛
        job.setNumReduceTasks(0);
        //6.指定程序输入路径
        FileInputFormat.setInputPaths(job,new Path("D:\\大数据资料\\maven\\day1\\linux_hadoop\\05_尚硅谷大数据技术之Hadoop\\2.资料\\09_测试数据\\input\\inputword"));
        //7输出路径
        FileOutputFormat.setOutputPath(job,new Path("D:\\tool\\da2"));
        //8.提交任务运行
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
