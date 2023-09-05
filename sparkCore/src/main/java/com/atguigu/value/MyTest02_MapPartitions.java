package com.atguigu.value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class MyTest02_MapPartitions {
    public static void main(String[] args) throws InterruptedException {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaRDD<Integer> intRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 2);

        JavaRDD<Integer> integerJavaRDD = intRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                System.out.println("调用了");
                ArrayList<Integer> arrayList = new ArrayList<>();
                while (integerIterator.hasNext()) {
                    Integer next = integerIterator.next();
                    arrayList.add(next*2);
                };
                return arrayList.iterator();
            }
        });

        integerJavaRDD.collect().forEach(System.out::println);


        JavaRDD<Integer> map1 = intRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i) throws Exception {
                System.out.println("map1");
                if (i % 2 == 1) {
                    return null;
                }
                return i * 2;
            }
        });

        map1.collect().forEach(System.out::println);

        Thread.sleep(6000000);
        // 4. 关闭sc
        sc.stop();

    }
}
