package com.atguigu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.TreeMap;

public class Top4 {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> textFile = sc.textFile("input/agent.log");

        JavaPairRDD<Tuple2<String, String>, Integer> rdd = textFile.mapToPair(new PairFunction<String, Tuple2<String,String>,
                Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(String s) throws Exception {
                String[] strings = s.split(" ");
                return new Tuple2<>(new Tuple2<>(strings[1],strings[4]), 1);
            }
        });

        //rdd求和
        JavaPairRDD<Tuple2<String, String>, Integer> reduceByKey = rdd.reduceByKey(new Function2<Integer
                , Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //把一个省的分一块
        JavaPairRDD<String, Tuple2<String, Integer>> proList =
                reduceByKey.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String
                        , Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> v) throws Exception {
                String pro = v._1._1;
                String id = v._1._2;
                Integer count = v._2;
                return new Tuple2<>(pro, new Tuple2<>(id, count));
            }
        });
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> province = proList.groupByKey();

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> res =
                province.mapValues(new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> v) throws Exception {
                TreeMap<Integer, String> treeMap = new TreeMap<>();
                //更改位置，数字放key的位置
                for (Tuple2<String, Integer> elm : v) {
                    if (treeMap.containsKey(elm._2)) {
                        treeMap.put(elm._2, treeMap.get(elm._2) + "_" + elm._1);
                    } else {
                        treeMap.put(elm._2, elm._1);
                    }
                }

                //挑前三
                ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
                while (list.size() != 3 && treeMap.size() != 0) {
                    Integer key = treeMap.lastEntry().getKey();
                    String value = treeMap.lastEntry().getValue();
                    if (value.contains("_")) {
                        String[] ids = value.split("_");
                        for (String id : ids) {
                            list.add(new Tuple2<>(id, key));
                        }
                    } else {
                        list.add(new Tuple2<>(value, key));
                    }
                    treeMap.remove(key);
                }
                return list;
            }
        });

        res.take(5).forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();

    }
}
