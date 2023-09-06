package com.atguigu.sparkTuning.aqe;

import com.atguigu.sparkTuning.utils.InitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.List;

/**
 * @author yhm
 * @create 2022-07-07 18:09
 */
public class AqeOptimizingSkewJoin {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]")
                .set("spark.sql.autoBroadcastJoinThreshold", "-1")  //为了演示效果，禁用广播join
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true") // 为了演示效果，关闭自动缩小分区
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.skewJoin.enable","true")
                .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor","2") // 中位数  默认是5
                .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes","20mb")  // 数据倾斜分区最小数据量
                .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8mb"); // 拆分分区之后的期望大小

        // 2. 创建sparkSession连接对象
        SparkSession sparkSession = InitUtil.initSparkSession(conf);

        // 3. 编写代码
        useJoin(sparkSession);

        Thread.sleep(600000);

        // 4. 关闭sparkSession
        sparkSession.close();
    }

    public static void useJoin(SparkSession sparkSession){
        Dataset<Row> saleCourse = sparkSession.sql("select *from sparktuning.sale_course");
        Dataset<Row>  coursePay = sparkSession.sql("select * from sparktuning.course_pay")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime");
        Dataset<Row>  courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart")
                .drop("coursename")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime");


        List<String> strings = Arrays.asList("courseid", "dt", "dn");
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(strings.iterator()).asScala().toSeq();

        List<String> strings1 = Arrays.asList("orderid", "dt", "dn");
        Seq<String> seq1 = JavaConverters.asScalaIteratorConverter(strings1.iterator()).asScala().toSeq();
        saleCourse.join(courseShoppingCart, seq, "right")
                //"orderid", "dt", "dn"
                .join(coursePay, seq1, "left")
                .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
                        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "dt", "dn")
                .write().saveAsTable("sparktuning.salecourse_detail_1");
    }
}
