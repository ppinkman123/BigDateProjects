package com.atguigu.wordcountdemo.amixup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ADriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ADriver.class);

        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ABean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ABean.class);

        job.setPartitionerClass(APartition.class);
        job.setNumReduceTasks(2);

        job.setOutputFormatClass(AOutPutFormat.class);

        FileInputFormat.setInputPaths(job,new Path("D:\\大数据资料\\maven\\day1\\linux_hadoop\\05_尚硅谷大数据技术之Hadoop\\2.资料\\09_测试数据\\input\\inputflow/phone_data.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:/output/mix2"));

        job.waitForCompletion(true);
    }
}
