package com.atguigu.key_value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class MyGBK {
    public static void main(String[] args) {
        // 1.创建配置对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

        // 2. 创建sparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 编写代码
        JavaPairRDD<String, Double> pairRDD1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("hello", 10.0), new Tuple2<>("world", 20.0),new Tuple2<>("hello", 11.0), new Tuple2<>("world", 21.0),new Tuple2<>("hello", 12.0), new Tuple2<>("world", 22.0),new Tuple2<>("hello", 10.0), new Tuple2<>("world", 20.0),new Tuple2<>("hello", 11.0), new Tuple2<>("world", 21.0),new Tuple2<>("hello", 12.0), new Tuple2<>("world", 22.0),new Tuple2<>("hello", 10.0), new Tuple2<>("world", 20.0),new Tuple2<>("hello", 11.0), new Tuple2<>("world", 21.0),new Tuple2<>("hello", 12.0), new Tuple2<>("world", 22.0),new Tuple2<>("hello", 10.0), new Tuple2<>("world", 20.0),new Tuple2<>("hello", 11.0), new Tuple2<>("world", 21.0),new Tuple2<>("hello", 12.0), new Tuple2<>("world", 22.0),new Tuple2<>("hello", 10.0), new Tuple2<>("world", 20.0),new Tuple2<>("hello", 11.0), new Tuple2<>("world", 21.0),new Tuple2<>("hello", 12.0), new Tuple2<>("world", 22.0)),2);

        JavaPairRDD<String, Iterable<Double>> PairRDD = pairRDD1.groupByKey();

        JavaPairRDD<String, Double> stringDoubleJavaPairRDD = PairRDD.mapValues(new Function<Iterable<Double>,
                Double>() {
            @Override
            public Double call(Iterable<Double> doubles) throws Exception {
                Double sum = 0.0;
                for (Double aDouble : doubles) {
                    sum += aDouble;
                }
                return sum;
            }
        });


        stringDoubleJavaPairRDD.collect().forEach(System.out::println);

        // 4. 关闭sc
        sc.stop();

    }
}
