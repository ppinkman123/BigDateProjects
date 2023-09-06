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

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;

/**
 * @author yhm
 * @create 2022-07-07 18:04
 */
public class SMBJoinTuning {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("SMBJoinTuning")
                .setMaster("local[*]")
                .set("spark.sql.shuffle.partitions", "36");

        SparkSession sparkSession = InitUtil.initSparkSession(sparkConf);

        useSMBJoin(sparkSession);

        Thread.sleep(600000);

    }

    public static void useSMBJoin(SparkSession sparkSession) {
        //查询出三张表 并进行join 插入到最终表中
//        sparkSession.sql("")
        Dataset<Row> saleCourse = sparkSession.sql("select *from sparktuning.sale_course");
        Dataset<Row> coursePay = sparkSession.sql("select * from sparktuning.course_pay_cluster")
                .withColumnRenamed("discount", "pay_discount")
                .withColumnRenamed("createtime", "pay_createtime");
        Dataset<Row> courseShoppingCart = sparkSession.sql("select *from sparktuning.course_shopping_cart_cluster")
                .drop("coursename")
                .withColumnRenamed("discount", "cart_discount")
                .withColumnRenamed("createtime", "cart_createtime");

        List<String> strings = Arrays.asList("orderid");
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(strings.iterator()).asScala().toSeq();
        List<String> strings1 = Arrays.asList("courseid");
        Seq<String> seq1 = JavaConverters.asScalaIteratorConverter(strings1.iterator()).asScala().toSeq();

        Dataset<Row> tmpdata = courseShoppingCart.join(coursePay, seq, "left");
        Dataset<Row> result = broadcast(saleCourse).join(tmpdata, seq1, "right");
        result
                .select("courseid", "coursename", "status", "pointlistid", "majorid", "chapterid", "chaptername", "edusubjectid"
                        , "edusubjectname", "teacherid", "teachername", "coursemanager", "money", "orderid", "cart_discount", "sellmoney",
                        "cart_createtime", "pay_discount", "paymoney", "pay_createtime", "sparktuning.sale_course.dt", "sparktuning.sale_course.dn")
                .write()
                .saveAsTable("sparktuning.salecourse_detail_2");
    }
}
