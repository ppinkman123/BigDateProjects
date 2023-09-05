package com.atguigu.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class MyWithIndex {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        JavaRDD<Tuple2<Integer, Integer>> tuple2JavaRDD = intRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, Integer>>>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Integer v1, Iterator<Integer> v2) throws Exception {
                ArrayList<Tuple2<Integer, Integer>> list = new ArrayList<>();
                while (v2.hasNext()) {
                    Integer next = v2.next();
                    list.add(new Tuple2<>(v1, next));
                }
                return list.iterator();
            }
        }, false);

        tuple2JavaRDD.collect().forEach(System.out::println);
        // 4. 关闭sc
        sc.stop();

    }
}
