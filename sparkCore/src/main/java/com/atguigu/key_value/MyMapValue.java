package com.atguigu.key_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class MyMapValue {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("hello", 10), new Tuple2<>("world", 20)));

        JavaPairRDD<String, String> stringStringJavaPairRDD = pairRDD.mapValues(new Function<Integer, String>() {
            @Override
            public String call(Integer integer) throws Exception {
                return integer + "$$$";
            }
        });
        stringStringJavaPairRDD.collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
