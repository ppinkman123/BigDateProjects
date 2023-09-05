package com.atguigu.sparksql;

import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

import static org.apache.spark.sql.functions.udaf;
/**
 * @作者：Icarus
 * @时间：2022/7/8 19:58
 */
public class UDAF02 {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 3. 编写代码
        Dataset<Row> userJson = spark.read().json("input/user.json");

        userJson.createOrReplaceTempView("user");

        spark.udf().register("myAvg",udaf(new MyAvg(),Encoders.LONG()));

        spark.sql("select name,myAvg(age) avgAge from user group by name").show();
        // 4. 关闭sparkSession
        spark.close();
    }

    @Data
    public static class Buffer implements Serializable{
        private Long sum ;
        private Long count;

        public Buffer(Long sum, Long count) {
            this.sum = sum;
            this.count = count;
        }

        public Buffer() {
        }
    }

    public static class  MyAvg extends Aggregator<Long,Buffer,Double>{

        public Buffer zero() {
            return new Buffer(0L,0L);
        }


        public Buffer reduce(Buffer res, Long elm) {
            res.setCount(res.getCount()+1);
            res.setSum(res.getSum()+elm);
            return res;
        }

        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }

        public Double finish(Buffer reduction) {
            return reduction.getSum().doubleValue()/reduction.getCount();
        }

        public Encoder<Buffer> bufferEncoder() {
            return Encoders.kryo(Buffer.class);
        }

        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
}
