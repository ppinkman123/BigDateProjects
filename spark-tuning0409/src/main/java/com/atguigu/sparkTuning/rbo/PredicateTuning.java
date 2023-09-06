package com.atguigu.sparkTuning.rbo;

import com.atguigu.sparkTuning.utils.InitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author yhm
 * @create 2022-07-06 16:28
 */
public class PredicateTuning {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = InitUtil.initSparkSession(conf);

        // 3. 编写代码
        spark.sql("use sparktuning;");

        System.out.println("=============Inner on 左表==============");
        spark.sql("select\n" +
                "  l.courseid,\n" +
                "  l.coursename,\n" +
                "  r.courseid,\n" +
                "  r.coursename\n" +
                "from sale_course l join course_shopping_cart r\n" +
                "  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn\n" +
                "  and l.courseid<2").explain("extended");

        System.out.println("============Inner where 左表===========");
        spark.sql("select\n" +
                "  l.courseid,\n" +
                "  l.coursename,\n" +
                "  r.courseid,\n" +
                "  r.coursename\n" +
                "from sale_course l join course_shopping_cart r\n" +
                "  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn\n" +
                "where l.courseid<2").explain("extended");


        System.out.println("===============left on 左表===============");
        spark.sql("select\n" +
                "  l.courseid,\n" +
                "  l.coursename,\n" +
                "  r.courseid,\n" +
                "  r.coursename\n" +
                "from sale_course l left join course_shopping_cart r\n" +
                "  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn\n" +
                "  and l.courseid<2").explain("extended");



        System.out.println("============left where 左表===========");
        spark.sql("select\n" +
                "  l.courseid,\n" +
                "  l.coursename,\n" +
                "  r.courseid,\n" +
                "  r.coursename\n" +
                "from sale_course l left join course_shopping_cart r\n" +
                "  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn\n" +
                "where l.courseid<2").explain("extended");



        System.out.println("============left on 右表===========");
        spark.sql("select\n" +
                "  l.courseid,\n" +
                "  l.coursename,\n" +
                "  r.courseid,\n" +
                "  r.coursename\n" +
                "from sale_course l left join course_shopping_cart r\n" +
                "  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn\n" +
                "  and r.courseid<2").explain("extended");

        System.out.println("============left where 右表===========");
        spark.sql("select\n" +
                "  l.courseid,\n" +
                "  l.coursename,\n" +
                "  r.courseid,\n" +
                "  r.coursename\n" +
                "from sale_course l left join course_shopping_cart r\n" +
                "  on l.courseid=r.courseid and l.dt=r.dt and l.dn=r.dn\n" +
                "where r.courseid<2 + 3").explain("extended");

        // 4. 关闭sparkSession
        spark.close();
    }
}
