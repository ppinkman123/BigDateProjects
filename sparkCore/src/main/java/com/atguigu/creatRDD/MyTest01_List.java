package com.atguigu.creatRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

public class MyTest01_List {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        final JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        intRDD.saveAsTextFile("out");

        JavaRDD<Integer> mapRDD = intRDD.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        mapRDD.collect().forEach(System.out::println);

        JavaRDD<Tuple2<String, Integer>> tuple2JavaRDD = intRDD.map(new Function<Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Integer v1) throws Exception {
                return new Tuple2<>("值为：", v1);
            }
        });
        tuple2JavaRDD.collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();


    }
}
