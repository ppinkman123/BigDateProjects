package com.atguigu.sparksql;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * @作者：Icarus
 * @时间：2022/7/8 20:35
 */
public class CSV03 {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        DataFrameReader reader = spark.read();

        Dataset<Row> userCsv = reader.option("header", "true")
                .option("sep", "\t")
                .csv("input/user.csv");

         userCsv.show();

        Dataset<User> userDS = userCsv.map(new MapFunction<Row, User>() {
            @Override
            public User call(Row row) throws Exception {
                return new User(Long.valueOf(row.getString(0)),
                        row.getString(1));
            }
        }, Encoders.bean(User.class));

        userDS.show();

        // 4. 关闭sparkSession
        spark.close();
    }

    @Data
    public static class User {
        private Long age;
        private String name;

        public User() {
        }

        public User(Long age, String name) {
            this.age = age;
            this.name = name;
        }
    }
}
