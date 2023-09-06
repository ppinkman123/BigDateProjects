package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.Properties;

/**
 * @author yhm
 * @create 2022-07-08 14:05
 */
public class Test10_MySQL {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        DataFrameReader reader = spark.read();

        Properties properties = new Properties();
        properties.setProperty("user","root");
        properties.setProperty("password","123456");

        Dataset<Row> userInfo = reader.jdbc("jdbc:mysql://hadoop102:3306/gmall", "user_info", properties);

        userInfo.printSchema();

        userInfo.show();

        // 编写简单的数据写出
        Dataset<Row> userJson = reader.json("input/user.json");

        DataFrameWriter<Row> writer = userJson.write();

        // 如果写出到mysql中  判断是否存在  依据的是表格
        writer.mode(SaveMode.Append)
                .jdbc("jdbc:mysql://hadoop102:3306/gmall","testInfo1",properties);

        // 4. 关闭sparkSession
        spark.close();
    }
}
