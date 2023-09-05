package com.atguigu.sparksql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @作者：Icarus
 * @时间：2022/7/8 22:33
 */
public class TT {
    public static void main(String[] args) {

        // 修改用户名  确认有权限
        System.setProperty("HADOOP_USER_NAME","atguigu");

        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        // .enableHiveSupport()开启连接hive的功能
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

        // 3. 编写代码
        Dataset<Row> showTables = spark.sql("show tables");

        showTables.show();


////        spark.sql("create table user_info (name string,age bigint)");
//        spark.sql("insert into table user_info values('zhangsan',18)");
//
//        Dataset<Row> userDS = spark.sql("select * from user_info");
//
////        userDS.write().saveAsTable("user_info1");
//
//        // sparkSQL写出到hive表格 有特殊的格式   需要使用sparkSQL来读取
//        spark.sql("select * from user_info1").show();

        // 4. 关闭sparkSession
        spark.close();
    }
}
