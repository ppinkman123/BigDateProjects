package com.atguigu.sparkTuning.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * @author yhm
 * @create 2022-07-06 11:36
 */
public class InitUtil {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("tuning").setMaster("local[*]");

        SparkSession ss = initSparkSession(conf);
        ss.sql("show tables").show();
        initHiveTable(ss);
        initBucketTable(ss);

    }

    public static SparkSession initSparkSession(SparkConf sparkConf){
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        SparkSession ss = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        SparkContext sc = ss.sparkContext();
        sc.hadoopConfiguration().set("fs.defaultFS", "hdfs://hadoop102:8020");
        return ss;
    }

    public static void initHiveTable(SparkSession ss){
        Dataset<Row> coursePay = ss.read().json("/sparkdata/coursepay.log");
        coursePay.write().partitionBy("dt", "dn")
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .saveAsTable("sparktuning.course_pay");

        Dataset<Row> saleCourse = ss.read().json("/sparkdata/salecourse.log");
        saleCourse.write().partitionBy("dt", "dn")
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .saveAsTable("sparktuning.sale_course");

        ss.read().json("/sparkdata/courseshoppingcart.log")
                .write().partitionBy("dt", "dn")
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .saveAsTable("sparktuning.course_shopping_cart");
    }

    public static void initBucketTable(SparkSession ss){
        ss.read().json("/sparkdata/coursepay.log")
                .write().partitionBy("dt", "dn")
                .format("parquet")
                .bucketBy(5, "orderid")
                .sortBy("orderid")
                .mode(SaveMode.Overwrite)
                .saveAsTable("sparktuning.course_pay_cluster");

        ss.read().json("/sparkdata/courseshoppingcart.log")
                .write().partitionBy("dt", "dn")
                .bucketBy(5, "orderid")
                .format("parquet")
                .sortBy("orderid")
                .mode(SaveMode.Overwrite)
                .saveAsTable("sparktuning.course_shopping_cart_cluster");
    }

}
