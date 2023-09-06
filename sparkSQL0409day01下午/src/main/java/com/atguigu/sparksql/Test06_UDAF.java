package com.atguigu.sparksql;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

import static org.apache.spark.sql.functions.udaf;

/**
 * @author yhm
 * @create 2022-07-08 9:28
 */
public class Test06_UDAF {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        Dataset<Row> userJson = spark.read().json("input/user.json");

        userJson.createOrReplaceTempView("user");

        // 注册udaf函数
        spark.udf().register("myAvg",udaf(new  MyAvg(),Encoders.LONG()));

        spark.sql("select name,myAvg(age) avgAge from user group by name").show();

        // 4. 关闭sparkSession
        spark.close();
    }

    @Data
    public static class Buffer implements Serializable {
        private Long sum;
        private Long count;

        public Buffer() {
        }

        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }
    }

    // 需要3个泛型
    // (1) IN 传入参数的类型  age -> long
    // (2) buffer 需要保存两个值 sum  count
    // (3) OUT 输出数值的类型  double
    public static class MyAvg extends Aggregator<Long,Buffer,Double>{

        /**
         * 初始化中间变量
         * @return
         */
        @Override
        public Buffer zero() {
            return new Buffer(0L,0L);
        }

        /**
         * 使用中间变量和每一行的值进行累加
         * @param b
         * @param a
         * @return
         */
        @Override
        public Buffer reduce(Buffer b, Long a) {
            b.setSum(b.getSum() + a);
            b.setCount(b.getCount() + 1);
            return b;
        }

        /**
         * 分区间合并
         * @param b1
         * @param b2
         * @return
         */
        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }

        /**
         * 最终根据中间变量计算出结果
         * @param reduction
         * @return
         */
        @Override
        public Double finish(Buffer reduction) {
            return reduction.getSum().doubleValue() / reduction.getCount();
        }

        /**
         * 确定序列化方法
         * @return
         */
        @Override
        public Encoder<Buffer> bufferEncoder() {
            // 使用bean兼容性更好  部分需求可以使用kryo进行优化
            return Encoders.kryo(Buffer.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
}
