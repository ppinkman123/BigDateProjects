package com.atguigu.hadoop.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
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
        conf.set("mapreduce.map.output.compress","true");
        conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.BZip2Codec");
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");
        // 开启map端输出压缩
        //conf.setBoolean("mapreduce.map.output.compress", true);
        //// 设置map端输出压缩方式
        //conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
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
        //// 设置reduce端输出压缩开启
        //FileOutputFormat.setCompressOutput(job, true);
        //
        //// 设置压缩的方式
        //FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        //6.指定程序输入路径
        FileInputFormat.setInputPaths(job,new Path("D:\\input\\inputbigwords"));
        //7.指定程序输出路径
        FileOutputFormat.setOutputPath(job,new Path("D:\\hadoop\\wc3"));
        //8.提交任务运行
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
