package com.atguigu.sparksql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author yhm
 * @create 2022-07-08 15:04
 */
public class Test01_Top3 {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

        // 3. 编写代码
        // 将3个表格数据join在一起
        Dataset<Row> t1DS = spark.sql("select \n" +
                "\tc.area,\n" +
                "\tc.city_name,\n" +
                "\tp.product_name\n" +
                "from\n" +
                "\tuser_visit_action u\n" +
                "join\n" +
                "\tcity_info c\n" +
                "on\n" +
                "\tu.city_id=c.city_id\n" +
                "join\n" +
                "\tproduct_info p\n" +
                "on\n" +
                "\tu.click_product_id=p.product_id");

        t1DS.createOrReplaceTempView("t1");

        // 将区域内的产品点击次数统计出来
        Dataset<Row> t2ds = spark.sql("select \n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tcount(*) counts\n" +
                "from\t\n" +
                "\tt1\n" +
                "group by\n" +
                "\tarea,product_name");

        t2ds.createOrReplaceTempView("t2");

        // 对区域内产品点击的次数进行排序  找出区域内的top3
        spark.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\trank() over (partition by area order by counts desc) rk\n" +
                "from \n" +
                "\tt2").createOrReplaceTempView("t3");

        // 使用过滤  取出区域内的top3
        spark.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\trk \n" +
                "from\n" +
                "\tt3\n" +
                "where \n" +
                "\trk < 4").show();

        // 4. 关闭sparkSession
        spark.close();
    }
}
