package com.atguigu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        // 1. 创建配置对象
        SparkConf conf = new SparkConf().setAppName("core").setMaster("yarn");

        // 2. 创建sc环境
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        // 读取数据
        JavaRDD<String> lineRDD = sc.textFile(args[0]);

        // 炸裂为单个单词
        JavaRDD<String> wordRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Arrays.asList(s1).iterator();
            }
        });

        // 转换为pairRDD  使用带有预聚合的reduceByKey进行合并
        JavaPairRDD<String, Integer> tupleOneRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });


        JavaPairRDD<String, Integer> result = tupleOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer res, Integer elem) throws Exception {
                return res + elem;
            }
        });

        result.saveAsTextFile(args[1]);

        // 4. 关闭sc
        sc.stop();
    }
}
