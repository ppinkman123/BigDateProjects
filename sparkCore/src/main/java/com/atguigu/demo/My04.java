package com.atguigu.demo;

import com.atguigu.bean.Bean;
import com.atguigu.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;


/**
 * @作者：Icarus
 * @时间：2022/7/6 16:11
 * 一次过
 */
public class My04 {
    public static void main(String[] args) {
    // 1.创建配置对象
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkCore");

    // 2. 创建sparkContext
    JavaSparkContext sc = new JavaSparkContext(conf);

    // 3. 编写代码
        JavaRDD<String> file = sc.textFile("input/user_visit_action.txt");

        JavaRDD<User> bigRDD = file.map(new Function<String, User>() {
            @Override
            public User call(String s) throws Exception {
                String[] data = s.split("_");
                return new User(
                        data[0],
                        data[1],
                        data[2],
                        data[3],
                        data[4],
                        data[5],
                        data[6],
                        data[7],
                        data[8],
                        data[9],
                        data[10],
                        data[11],
                        data[12]
                );
            }
        });

        JavaRDD<Bean> smallRDD = bigRDD.flatMap(new FlatMapFunction<User, Bean>() {
            @Override
            public Iterator<Bean> call(User user) throws Exception {
                ArrayList<Bean> list = new ArrayList<>();
                if (!("-1".equals(user.getClick_category_id()))) {
                    list.add(new Bean(user.getClick_category_id(), 1L, 0L, 0L));
                } else if (!("null".equals(user.getOrder_category_ids()))) {
                    String[] split = user.getOrder_category_ids().split(",");
                    for (String s : split) {
                        list.add(new Bean(s, 0L, 1L, 0L));
                    }
                } else if (!("null".equals(user.getPay_category_ids()))) {
                    String[] split = user.getPay_category_ids().split(",");
                    for (String s : split) {
                        list.add(new Bean(s, 0L, 0L, 1L));
                    }
                }
                return list.iterator();
            }
        });

        JavaPairRDD<String, Bean> smallToPair = smallRDD.mapToPair(new PairFunction<Bean, String, Bean>() {
            @Override
            public Tuple2<String, Bean> call(Bean bean) throws Exception {
                return new Tuple2<>(bean.getCategoryId(), bean);
            }
        });

        JavaPairRDD<String, Bean> sumSmallRDD = smallToPair.reduceByKey(new Function2<Bean, Bean, Bean>() {
            @Override
            public Bean call(Bean bean, Bean bean2) throws Exception {
                bean.setClickCount(bean.getClickCount() + bean2.getClickCount());
                bean.setOrderCount(bean.getOrderCount() + bean2.getOrderCount());
                bean.setPayCount(bean.getPayCount() + bean2.getPayCount());
                return bean;
            }
        });

        JavaRDD<Tuple2<String, Double>> avgSmallRDD = sumSmallRDD.map(new Function<Tuple2<String, Bean>, Tuple2<String,
                Double>>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Bean> s) throws Exception {
                return new Tuple2<>(s._1,
                        s._2.getClickCount() * 0.2 + s._2.getOrderCount() * 0.3 + s._2.getPayCount() * 0.5);
            }
        });

        JavaRDD<Tuple2<String, Double>> res = avgSmallRDD.sortBy(new Function<Tuple2<String, Double>,
                Double>() {
            @Override
            public Double call(Tuple2<String, Double> s) throws Exception {
                return s._2;
            }
        }, false, 2);


        res.take(10).forEach(System.out::println);
        // 4. 关闭sc
    sc.stop();

    }
}
