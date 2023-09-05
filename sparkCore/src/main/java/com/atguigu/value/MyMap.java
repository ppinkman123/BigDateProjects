package com.atguigu.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class MyMap {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        JavaRDD<Tuple2<String, Integer>> mapRDD = intRDD.map(
                (Function<Integer, Tuple2<String, Integer>>) integer -> new Tuple2<>("值：",integer));

        mapRDD.collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
