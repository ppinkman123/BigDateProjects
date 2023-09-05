package com.atguigu.key_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

public class MyS {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Integer> pairRDD1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("hello", 10), new Tuple2<>
                ("world", 280),new Tuple2<>("hi", 210),new Tuple2<>("haha", 220)),2);

        JavaPairRDD<String, Integer> result = pairRDD1.sortByKey();

        if(0==1){
            JavaPairRDD<Integer, Tuple2<String, Integer>> sort =
                    pairRDD1.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<Integer, Tuple2<String, Integer>> call(Tuple2<String, Integer> v1) throws Exception {
                    return new Tuple2<>(v1._2, v1);
                }
            }).sortByKey(false);

            JavaPairRDD<String, Integer> rdd = sort.mapToPair(new PairFunction<Tuple2<Integer,
                    Tuple2<String, Integer>>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<Integer, Tuple2<String, Integer>> v1) throws Exception {
                    return v1._2;
                }
            });

        }

        result.collect().forEach(System.out::println);


        Thread.sleep(600000);
        // 4. 关闭sc
        sc.stop();

    }
}
