package com.atguigu.key_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;

public class MyReBK {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> pairRDD1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("hello", 10), new Tuple2<>("world", 20),new Tuple2<>("hello", 11), new Tuple2<>("world", 21),new Tuple2<>("hello", 12), new Tuple2<>("world", 22)),2);

        JavaPairRDD<String, Tuple2<Integer, Integer>> pairRDD =
                pairRDD1.mapValues(new Function<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) throws Exception {
                return new Tuple2<>(i, 1);
            }
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> pairRDD2 =
                pairRDD.reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer,
                Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Tuple2<Integer,
                    Integer> y) throws Exception {
                return new Tuple2<>(x._1 + y._1, x._2 + y._2);
            }
        });

        JavaPairRDD<String, Double> rdd = pairRDD2.mapValues(new Function<Tuple2<Integer,
                Integer>, Double>() {
            @Override
            public Double call(Tuple2<Integer, Integer> a) throws Exception {
                return  a._1.doubleValue() / a._2;
            }
        });
        rdd.collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
