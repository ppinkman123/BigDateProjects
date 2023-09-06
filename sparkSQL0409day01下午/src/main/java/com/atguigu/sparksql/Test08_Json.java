package com.atguigu.sparksql;

import com.atguigu.sparksql.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

/**
 * @author yhm
 * @create 2022-07-08 11:02
 */
public class Test08_Json {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        DataFrameReader reader = spark.read();

        Dataset<Row> userJson = reader.json("input/user.json");

        userJson.printSchema();

        Dataset<User> userDS = userJson.as(Encoders.bean(User.class));

        userDS.show();

        DataFrameWriter<User> writer = userDS.write();

        // spark的数据写出不用和之前的格式对象  可以写出为任意格式
        writer.mode(SaveMode.Append)
                .json("output");

        writer.mode(SaveMode.Append)
                .csv("output");

        // 4. 关闭sparkSession
        spark.close();
    }
}
