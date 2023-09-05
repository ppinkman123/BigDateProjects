package com.atguigu.key_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;

public class MyPair {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        JavaPairRDD<Object, Object> pairRDD = rdd.mapToPair(new PairFunction<Integer, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(Integer integer) throws Exception {
                return new Tuple2<>(integer, integer * 2);
            }
        });

        pairRDD.collect().forEach(System.out::println);

        JavaPairRDD<String, Integer> nihao = sc.parallelizePairs(Arrays.asList(new Tuple2<>("nihao", 4),new Tuple2<>("po",4)));
        nihao.collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
