package com.atguigu.wordcountdemo.ouformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.io.IOException;

public class LRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguFos;
    private  FSDataOutputStream otherFos;

    public LRecordWriter(TaskAttemptContext job) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(job.getConfiguration());
            atguiguFos = fs.create(new Path("D:\\log\\atguigu.txt"));
            otherFos = fs.create(new Path("D:\\log\\other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if(log.contains("atguigu")){
            atguiguFos.writeBytes(log+"\n");
        }else {
            otherFos.writeBytes(log+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(atguiguFos,otherFos);
    }
}
