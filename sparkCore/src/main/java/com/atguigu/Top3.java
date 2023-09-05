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


public class Top3 {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<String> lineRDD = sc.textFile("input/agent.log");

        // 转换格式
        JavaPairRDD<Tuple2<String, String>, Integer> pairRDD = lineRDD.mapToPair(new PairFunction<String, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                Tuple2<String, String> key = new Tuple2<>(s1[1], s1[4]);
                return new Tuple2<>(key, 1);
            }
        });

        // 聚合统计
        JavaPairRDD<Tuple2<String, String>, Integer> reduceByKeyRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 转换结构省份为key
        JavaPairRDD<String, Tuple2<String, Integer>> provinceCountRDD = reduceByKeyRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                // 省份
                String key = tuple2IntegerTuple2._1._1;
                // (广告id,count)
                Tuple2<String, Integer> value = new Tuple2<>(tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2);
                return new Tuple2<>(key, value);
            }
        });

        // 将相同省份的聚合
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> provinceRDD = provinceCountRDD.groupByKey();

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> result = provinceRDD.mapValues(new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
            @Override
            public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> v1) throws Exception {

                TreeMap<Integer, String> treeMap = new TreeMap<>();

                for (Tuple2<String, Integer> elem : v1) {
                    // 直接放相同值会覆盖
                    if (treeMap.containsKey(elem._2)) {
                        // 已经存在相同的key  拼接value
                        treeMap.put(elem._2, treeMap.get(elem._2) + "__" + elem._1);
                    } else {
                        treeMap.put(elem._2, elem._1);
                    }
                }

                // 获取排名前三的广告id
                ArrayList<Tuple2<String, Integer>> tuple2s = new ArrayList<>();
                while (tuple2s.size() != 3 && treeMap.size() != 0) {
                    Integer key = treeMap.lastEntry().getKey();
                    String value = treeMap.lastEntry().getValue();

                    if (value.contains("__")) {
                        String[] ids = value.split("__");
                        for (String id : ids) {
                            tuple2s.add(new Tuple2<>(id, key));
                        }
                    } else {
                        tuple2s.add(new Tuple2<>(value, key));
                    }
                    treeMap.remove(key);
                }
                return tuple2s;
            }
        });

        result. collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();

    }


}
