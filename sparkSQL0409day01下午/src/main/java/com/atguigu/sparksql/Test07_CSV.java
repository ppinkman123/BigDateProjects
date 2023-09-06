package com.atguigu.sparksql;

import com.atguigu.sparksql.bean.User;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * @author yhm
 * @create 2022-07-08 10:15
 */
public class Test07_CSV {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        DataFrameReader reader = spark.read();

        // 默认不读列名
        // 添加参数读取列名
        Dataset<Row> userCsv = reader.option("header", "true") // 修改为true会将第一行数据作为列名
                .option("sep", "\t")
                // 不用填写输入文件的压缩格式  会自动识别
                .csv("input/user.csv");

        userCsv.show();

        // 将csv的文件转换为User
        // csv读取的文件只能将数据识别为string
//        Dataset<User> dataset = userCsv.as(Encoders.bean(User.class));
        // 转换可以使用map
        Dataset<User> userDS = userCsv.map(new MapFunction<Row, User>() {
            @Override
            public User call(Row value) throws Exception {

                return new User(Long.valueOf(value.getString(0)), value.getString(1));
            }
        }, Encoders.bean(User.class));

        userDS.show();

        // 写出csv文件
        DataFrameWriter<User> writer = userDS.write();

        // 直接写出使用默认的参数  分隔符为,  同时没有列名
        writer.option("header", "true")
                .option("sep", ";")
                .option("compression","gzip") // 确定压缩格式
                // 4种输出模式
                // (1) 默认ErrorIfExists 如果输出路径存在报错
                // (2) append 追加写  在文件夹后面从新写一个新文件
                // (3) Overwrite 覆盖写  将之前的全部删除  重新写入
                // (4) Ignore 忽略  如果目标路径已经存在  忽略当前写出操作
                .mode(SaveMode.Ignore)
                .csv("output");

        // 4. 关闭sparkSession
        spark.close();
    }
}
