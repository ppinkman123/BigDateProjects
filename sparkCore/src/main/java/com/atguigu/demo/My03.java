package com.atguigu.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
/**
一遍过
 */
public class My03 {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> file = sc.textFile("input/user_visit_action.txt");

        //炸裂开文件
        JavaPairRDD<String, Double> splitPair = file.flatMapToPair(new PairFlatMapFunction<String,
                String, Double>() {
            @Override
            public Iterator<Tuple2<String, Double>> call(String s) throws Exception {
                String[] data = s.split("_");
                ArrayList<Tuple2<String, Double>> res = new ArrayList<>();
                if (!"-1".equals(data[6])) {
                    res.add(new Tuple2<>(data[6], 0.2));
                } else if (!"null".equals(data[8])) {
                    String[] split = data[8].split(",");
                    for (String s1 : split) {
                        res.add(new Tuple2<>(s1, 0.3));
                    }
                } else if (!"null".equals(data[10])) {
                    String[] split = data[10].split(",");
                    for (String s1 : split) {
                        res.add(new Tuple2<>(s1, 0.5));
                    }
                }
                return res.iterator();
            }
        });
        //求和
        JavaPairRDD<String, Double> sumRDD = splitPair.reduceByKey(new Function2<Double, Double,
                Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });

        //改变结构排序
        JavaRDD<Tuple2<String, Double>> res =
                sumRDD.map(new Function<Tuple2<String, Double>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Double> s) throws Exception {
                double value = s._2.doubleValue();
                String format = String.format("%.2f", value);
                return new Tuple2<>(s._1,Double.valueOf(format));
            }
        });

        //找前十
        JavaRDD<Tuple2<String, Double>> result = res.sortBy(new Function<Tuple2<String, Double>, Double>() {
            @Override
            public Double call(Tuple2<String, Double> s) throws Exception {
                return s._2;
            }
        }, false, 2);

        result.take(10).forEach(System.out::println);
        Thread.sleep(990000);

        // 4. 关闭sc
        sc.stop();

    }
}
