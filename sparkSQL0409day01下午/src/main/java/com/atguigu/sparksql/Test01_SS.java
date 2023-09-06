package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author yhm
 * @create 2022-07-06 14:40
 */
public class Test01_SS {
    public static void main(String[] args) {
            // 1. 创建sparkConf配置对象
            SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

            // 2. 创建sparkSession连接对象
            SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

            // 3. 编写代码

            // 4. 关闭sparkSession
            spark.close();
    }
}
