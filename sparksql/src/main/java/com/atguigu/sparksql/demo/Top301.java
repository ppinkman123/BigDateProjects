package com.atguigu.sparksql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @作者：Icarus
 * @时间：2022/7/8 21:33
 */
public class Top301 {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        System.setProperty("HADOOP_USER_NAME","atguigu");

        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

        // 3. 编写代码
        Dataset<Row> dataset = spark.sql("select\n" +
                "    c.area,\n" +
                "    c.city_name,\n" +
                "    p.product_name\n" +
                "    from\n" +
                "    user_visit_action u\n" +
                "    join\n" +
                "    city_info c\n" +
                "    on\n" +
                "    u.city_id = c.city_id\n" +
                "    join\n" +
                "    product_info p\n" +
                "    on \n" +
                "    u.click_product_id = p.product_id");

        dataset.createOrReplaceTempView("t1");

        spark.sql("select\n" +
                "  area,\n" +
                "  product_name,\n" +
                "  count(*) counts\n" +
                "from\n" +
                "t1\n" +
                "group by\n" +
                "area,product_name").show();




//        dataset.show();



        // 4. 关闭sparkSession
        spark.close();
    }
}
