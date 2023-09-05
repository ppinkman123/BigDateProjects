package com.atguigu.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;

public class My01 {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> lineRDD = sc.textFile("input/user_visit_action.txt");

        JavaRDD<String> clickRDD = lineRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] data = s.split("_");

                return !("-1").equals(data[6]);
            }
        });

        JavaPairRDD<String, Integer> clickTupleOneRDD = clickRDD.mapToPair(new PairFunction<String, String,
                Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] data = s.split("_");
                return new Tuple2<>(data[6], 1);
            }
        });

        JavaPairRDD<String, Integer> clickCountRDD = clickTupleOneRDD.reduceByKey(new Function2<Integer,
                Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        JavaRDD<String> orderRDD = lineRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] data = v1.split("_");

                return !("null".equals(data[8]));
            }
        });

        JavaPairRDD<String, Integer> orderTupleOneRDD = orderRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] data = s.split("_");
                String[] orders = data[8].split(",");
                ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();

                for (String order : orders) {
                    result.add(new Tuple2<>(order, 1));
                }
                return result.iterator();
            }
        });

        JavaPairRDD<String, Integer> orderCountRDD = orderTupleOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaRDD<String> payRDD = lineRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] data = v1.split("_");
                return !("null".equals(data[10]));
            }
        });

        JavaPairRDD<String, Integer> payTupleOneRDD = payRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] data = s.split("_");
                String[] pays = data[10].split(",");
                ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
                for (String pay : pays) {
                    result.add(new Tuple2<>(pay, 1));
                }

                return result.iterator();
            }
        });

        JavaPairRDD<String, Integer> payCountRDD = payTupleOneRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 步骤2: 使用算子合并  计算出最终的结果即可
        JavaPairRDD<String, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> categoryRDD = clickCountRDD.cogroup(orderCountRDD, payCountRDD);

        JavaPairRDD<String, Double> categoryCountRDD = categoryRDD.mapValues(new Function<Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>, Double>() {
            @Override
            public Double call(Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>> v1) throws Exception {
                Double click = 0.0;
                Double order = 0.0;
                Double pay = 0.0;
                Iterator<Integer> clickIt = v1._1().iterator();
                Iterator<Integer> orderIt = v1._2().iterator();
                Iterator<Integer> payIt = v1._3().iterator();
                if (clickIt.hasNext()) {
                    click = clickIt.next().doubleValue();
                }
                if (orderIt.hasNext()) {
                    order = orderIt.next().doubleValue();
                }
                if (payIt.hasNext()) {
                    pay = payIt.next().doubleValue();
                }
                double value = click * 0.2 + order * 0.3 + pay * 0.5;
                String format = String.format("%.2f", value);
                return Double.valueOf(format);
            }
        });

        JavaPairRDD<Double, Tuple2<String, Double>> sortByKeyRDD = categoryCountRDD.mapToPair(new PairFunction<Tuple2<String, Double>, Double, Tuple2<String, Double>>() {
            @Override
            public Tuple2<Double, Tuple2<String, Double>> call(Tuple2<String, Double> v1) throws Exception {
                return new Tuple2<>(v1._2, v1);
            }
        }).sortByKey(false);

        JavaRDD<Tuple2<String, Double>> result = sortByKeyRDD.map(new Function<Tuple2<Double, Tuple2<String, Double>>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<Double, Tuple2<String, Double>> v1) throws Exception {
                return v1._2;
            }
        });

        result.take(10).forEach(System.out::println);
        Thread.sleep(600000);
        // 4. 关闭sc
        sc.stop();

    }
}
