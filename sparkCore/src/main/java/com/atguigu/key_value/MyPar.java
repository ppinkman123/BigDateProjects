package com.atguigu.key_value;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class MyPar {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<Integer, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>(2, 1), new Tuple2<>(18, 1), new Tuple2<>(10, 1), new Tuple2<>(10, 1), new Tuple2<>(12, 1)),3);

        JavaPairRDD<Integer, Integer> pairRDD1 = pairRDD.partitionBy(new HashPartitioner(3));
        pairRDD1.saveAsTextFile("out");

        Thread.sleep(90000000);

        // 4. 关闭sc
        sc.stop();

    }
}
