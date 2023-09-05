package com.atguigu.hadoop.mapreduce.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author clh
 * @create 2022-06-02-15:21
 */
public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private HashMap<String, String> pdMap;
    private Text outK=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //0.创建HashMap结构装商品的数据
        pdMap = new HashMap<>();
        //1.通过上下文对象来获取缓存
        URI[] cacheFiles = context.getCacheFiles();
        URI cacheFile = cacheFiles[0];
        //2.通过uri来读取数据  开流读数据
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fdis = fs.open(new Path(cacheFile));
        //3.开字符流读数据 //装饰者设计模式 只有最外一层对象被使用时 里面的对象才会被创建
        BufferedReader reader = new BufferedReader(new InputStreamReader(fdis, "UTF-8"));
        String line;
        while ((line=reader.readLine())!=null){
            String[] pdInfo = line.split("\t");
            pdMap.put(pdInfo[0],pdInfo[1]);
        }
        //关闭资源
        IOUtils.closeStreams(reader,fdis);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //转化数据
        String line = value.toString();
        String[] orderInfo = line.split("\\t");
        //通过连接键来获取小表集合中对应的pname
        String pid = orderInfo[1];
        String pname = pdMap.get(pid);
        //封装写出
        outK.set(orderInfo[0]+"\t"+pname+"\t"+orderInfo[2]);
        context.write(outK,NullWritable.get());
    }
}
