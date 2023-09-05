package com.atguigu.wordcountdemo.ouformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LDriver{
public static void main(String[]args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(LDriver.class);
        job.setMapperClass(LMapper.class);
        job.setReducerClass(LReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置自定义的outputformat
        job.setOutputFormatClass(LOutputformat.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\大数据资料\\maven\\day1\\linux_hadoop\\05_尚硅谷大数据技术之Hadoop\\2.资料\\09_测试数据\\input\\inputoutputformat"));
        //虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        //而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("D:\\log1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
        }
}
