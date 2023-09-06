package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

/**
 * @author yhm
 * @create 2022-07-08 11:08
 */
public class Test09_Parquet {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        DataFrameReader reader = spark.read();
        Dataset<Row> json = reader.json("input/user.json");

        DataFrameWriter<Row> writer = json.write();

        // 默认使用snappy压缩  特点快
        writer.mode(SaveMode.Overwrite)
                .parquet("output");

        // parquet具有自解析的功能
        Dataset<Row> userParquet = reader.parquet("output");
        userParquet.printSchema();
        userParquet.show();

        // 4. 关闭sparkSession
        spark.close();
    }
}
