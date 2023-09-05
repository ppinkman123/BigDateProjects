package com.atguigu.wordcountdemo.amixup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class ARecordWriter extends RecordWriter<Text,ABean> {

    private  FSDataOutputStream my;
    private  FSDataOutputStream oth;

    public ARecordWriter(TaskAttemptContext job) {
        Configuration conf = job.getConfiguration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            my = fs.create(new Path("D:/output/fenqu/my"));
            oth = fs.create(new Path("D:/output/fenqu/other"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, ABean value) throws IOException, InterruptedException {
        String prv = key.toString().substring(0, 3);
        String s = key.toString() + "\t" + value.toString()+"\n";
        if(prv.contains("136")){
            my.writeBytes(s);
        }else {
            oth.writeBytes(s);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(my,oth);
    }
}
