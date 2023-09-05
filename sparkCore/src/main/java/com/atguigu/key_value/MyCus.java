package com.atguigu.key_value;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class MyCus {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> pairRDD1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("hello", 10),
                new Tuple2<>("world", 20)));
        JavaPairRDD<Integer, Integer> pairRDD2 = sc.parallelizePairs(Arrays.asList(new Tuple2<>(2, 1), new Tuple2<>(18, 1), new Tuple2<>(10, 1), new Tuple2<>(10, 1), new Tuple2<>(12, 1)));

        JavaPairRDD<Integer, Integer> pairRDD = pairRDD2.partitionBy(new Zidingyi(3));

        pairRDD.saveAsTextFile("out");
//        pairRDD1.partitionBy();
        // 4. 关闭sc
        sc.stop();

    }
}
