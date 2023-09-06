package com.atguigu.sparkTuning.aqe;

import com.atguigu.sparkTuning.utils.InitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

/**
 * @author yhm
 * @create 2022-07-11 17:00
 */
public class AqeDynamicSwitchJoin {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("AqeDynamicSwitchJoin")
                .setMaster("local[*]")
                .set("spark.sql.adaptive.enabled", "true")
                //在不需要进行shuffle重分区时，尝试使用本地shuffle读取器。将sort-meger join 转换为广播join
                .set("spark.sql.adaptive.localShuffleReader.enabled", "true");

        SparkSession sparkSession = InitUtil.initSparkSession(conf);

        switchJoinStartegies(sparkSession);

        Thread.sleep(600000);
    }

    public static void switchJoinStartegies(SparkSession sparkSession){
        Dataset<Row> coursePay = sparkSession.sql("select * from sparktuning.course_pay")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime")
                .where("orderid between 'odid-9999000' and 'odid-9999999'");

        Dataset<Row> courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart")
                .drop("coursename")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime");

        List<String> strings = Arrays.asList("orderid");
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(strings.iterator()).asScala().toSeq();

        Dataset<Row> tmpdata = coursePay.join(courseShoppingCart, seq, "right");

        tmpdata.explain();
        tmpdata.show();
    }
}
