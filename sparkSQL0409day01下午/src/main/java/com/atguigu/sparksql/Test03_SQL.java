package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author yhm
 * @create 2022-07-06 15:25
 */
public class Test03_SQL {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        Dataset<Row> jsonDS = spark.read().json("input/user.json");

        // OrReplace  覆盖的意思
        // Global 整个application都能用  不加  当前的sparkSession能用
        jsonDS.createOrReplaceTempView("user_info");


        Dataset<Row> rowDataset = spark.sql("select age+1 newAge,name from user_info where age >= 18");

        rowDataset.createOrReplaceTempView("t1");

        // 下面的sql可以from t1

        rowDataset.show();

        // 4. 关闭sparkSession
        spark.close();
    }
}
