package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import static org.apache.spark.sql.functions.col;
/**
 * @author yhm
 * @create 2022-07-06 15:31
 */
public class Test04_DSL {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        Dataset<Row> jsonDS = spark.read().json("input/user.json");

        // 导入特殊的依赖 import static org.apache.spark.sql.functions.col;
        jsonDS.select(col("age").plus(1).as("newAge"),col("name"))
                .filter(col("age").gt(18))
                .show();

        // 4. 关闭sparkSession
        spark.close();
    }
}
