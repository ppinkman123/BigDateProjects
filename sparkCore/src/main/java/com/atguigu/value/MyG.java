package com.atguigu.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

public class MyG {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7), 2);

        JavaPairRDD<Boolean, Iterable<Integer>> booleanIterableJavaPairRDD = rdd.groupBy(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });

        JavaPairRDD<Integer, Iterable<Integer>> integerIterableJavaPairRDD = rdd.groupBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer % 3;
            }
        },6);

        integerIterableJavaPairRDD.collect().forEach(System.out::println);

//        booleanIterableJavaPairRDD.collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
