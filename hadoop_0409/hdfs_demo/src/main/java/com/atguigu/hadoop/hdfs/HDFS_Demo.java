package com.atguigu.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFS_Demo {


    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://hadoop102:8020");
        URI uri = new URI("hdfs://hadoop102:8020");
        //获取文件系统对象
        FileSystem fs = FileSystem.get(uri,conf,"atguigu");
        //操作
        fs.mkdirs(new Path("/sppjava"));
        //关闭
        fs.close();

    }
}
