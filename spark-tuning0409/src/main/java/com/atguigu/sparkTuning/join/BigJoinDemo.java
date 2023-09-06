package com.atguigu.sparkTuning.join;

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
 * @create 2022-07-07 17:59
 */
public class BigJoinDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("BigJoinDemo")
                .setMaster("local[*]")
                .set("spark.sql.shuffle.partitions", "36");

        SparkSession spark = InitUtil.initSparkSession(conf);

        useJoin(spark);

        Thread.sleep(600000);

    }

    public static void useJoin(SparkSession sparkSession){
        //查询出三张表 并进行join 插入到最终表中
        Dataset<Row> saleCourse = sparkSession.sql("select * from sparktuning.sale_course");

        Dataset<Row> coursePay = sparkSession.sql("select * from sparktuning.course_pay")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime");

        Dataset<Row> courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart")
                .drop("coursename")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime");

        List<String> strings = Arrays.asList("orderid");
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(strings.iterator()).asScala().toSeq();
        List<String> strings1 = Arrays.asList("courseid");
        Seq<String> seq1 = JavaConverters.asScalaIteratorConverter(strings1.iterator()).asScala().toSeq();

        courseShoppingCart
                .join(coursePay, seq, "left")
                .join(saleCourse, seq1, "right")
                .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
                        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "sparktuning.sale_course.dt", "sparktuning.sale_course.dn")
                .write().saveAsTable("sparktuning.salecourse_detail_1");

    }




}
