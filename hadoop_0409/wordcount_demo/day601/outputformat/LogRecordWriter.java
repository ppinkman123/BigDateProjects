package com.atguigu.hadoop.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author clh
 * @create 2022-06-01-11:36
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguFos;
    private  FSDataOutputStream otherFos;

    public LogRecordWriter(TaskAttemptContext job) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(job.getConfiguration());
            atguiguFos = fs.create(new Path("D:\\hadoop\\log\\atguigu.txt"));
            otherFos = fs.create(new Path("D:\\hadoop\\log\\other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 每一个reduce的输出调用一次
     * @param key   log
     * @param value 空值
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
       //转化字符串
        String log = key.toString();
        if (log.contains("atguigu")){
            atguiguFos.writeBytes(log+"\n");
        }else{
            otherFos.writeBytes(log+"\n");

        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(otherFos,atguiguFos);
    }
}
