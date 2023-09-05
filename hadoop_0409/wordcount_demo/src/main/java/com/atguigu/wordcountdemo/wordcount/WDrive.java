package com.atguigu.wordcountdemo.wordcount;

import com.atguigu.wordcountdemo.combiner.WordCountDriver;
import com.atguigu.wordcountdemo.combiner.WordCountMapper;
import com.atguigu.wordcountdemo.combiner.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WDrive {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.通过配置文件，来获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2.绑定driver(本地) 或者 绑定jar包(yarn)
        job.setJarByClass(WDrive.class);
        //3.绑定mapper和reducer
        job.setMapperClass(WMap.class);
        job.setReducerClass(WCount.class);
        //4.指定Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5.指定最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置combiner
//       job.setCombinerClass(Combiner.class);
        //需要reduce嘛
        job.setNumReduceTasks(1);
        //6.指定程序输入路径
        FileInputFormat.setInputPaths(job,new Path("D:\\大数据资料\\maven\\day1\\linux_hadoop\\05_尚硅谷大数据技术之Hadoop\\2.资料\\09_测试数据\\input\\inputword"));
        //7输出路径
        FileOutputFormat.setOutputPath(job,new Path("D:\\output\\count"));
        //8.提交任务运行
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
