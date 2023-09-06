package com.atguigu.sparksql;

import com.atguigu.sparksql.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * @author yhm
 * @create 2022-07-06 15:08
 */
public class Test02_Method {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        DataFrameReader reader = spark.read();

        Dataset<Row> rowDS = reader.json("input/user.json");

        rowDS.printSchema();

        rowDS.show();

        // 处理数据
        // 需求: 让user的年龄+1岁
        Dataset<Row> rowDS1 = rowDS.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                long age = value.getLong(0);
                String name = value.getString(1);
                Row row = RowFactory.create(age, name);
                return row;
            }
        },Encoders.kryo(Row.class));

        rowDS1.show();

        // 推荐使用的方法  转换为自定义的javaBean
        Dataset<User> userDS1 = rowDS.as(Encoders.bean(User.class));


        Dataset<User> userDS = rowDS.map(new MapFunction<Row, User>() {
            @Override
            public User call(Row value) throws Exception {
                return new User(value.getLong(0) + 1, value.getString(1));
            }
        }, Encoders.bean(User.class));

        userDS.show();


        // 非函数式的语法
        Dataset<Row> sortDS = rowDS.sort("age");
        sortDS.show();


        // 4. 关闭sparkSession
        spark.close();
    }
}
