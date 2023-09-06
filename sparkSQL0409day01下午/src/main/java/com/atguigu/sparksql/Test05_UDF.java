package com.atguigu.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.udf;

/**
 * @author yhm
 * @create 2022-07-08 9:11
 */
public class Test05_UDF {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        // 需求: 将name后面添加大侠

        // 创建一个函数
        UserDefinedFunction udf = udf(new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s + " 大侠";
            }
        }, DataTypes.StringType);

        // 注册函数
        spark.udf().register("addName",udf);

        Dataset<Row> userJson = spark.read().json("input/user.json");

        userJson.createOrReplaceTempView("user");

        spark.sql("select addName(name) newName,age  from user").show();

        // 拼接两列  为 xx大侠 xx岁
        UserDefinedFunction udf1 = udf(new UDF2<String, Long, String>() {
            @Override
            public String call(String s, Long aLong) throws Exception {
                return s + " 大侠" + aLong + "岁";
            }
        }, DataTypes.StringType);
        // 注册函数
        spark.udf().register("addName1",udf1);

        spark.sql("select addName1(name,age) newName from user").show();

        // 4. 关闭sparkSession
        spark.close();
    }
}
