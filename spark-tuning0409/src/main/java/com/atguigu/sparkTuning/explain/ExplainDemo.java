package com.atguigu.sparkTuning.explain;

import com.atguigu.sparkTuning.utils.InitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author yhm
 * @create 2022-07-06 11:54
 */
public class ExplainDemo {
    public static void main(String[] args) {
        //1. 创建配置对象
        SparkConf conf = new SparkConf().setAppName("sparksql").setMaster("local[*]");

        //2. 获取sparkSession
        SparkSession sparkSession = InitUtil.initSparkSession(conf);
        sparkSession.sql("use sparktuning;");
        //3. 编写代码
        String sqlstr = "select\n" +
                "  sc.courseid,\n" +
                "  sc.coursename,\n" +
                "  sum(sellmoney) as totalsell\n" +
                "from sale_course sc join course_shopping_cart csc\n" +
                "  on sc.courseid=csc.courseid and sc.dt=csc.dt and sc.dn=csc.dn\n" +
                "group by sc.courseid,sc.coursename";



        System.out.println("==================explain()-只展示物理执行计划=====================");
        sparkSession.sql(sqlstr).explain();

        System.out.println(("===============================explain(mode = \"simple\")-只展示物理执行计划================================="));
        sparkSession.sql(sqlstr).explain("simple");

        System.out.println(("============================explain(mode = \"extended\")-展示逻辑和物理执行计划=============================="));
        sparkSession.sql(sqlstr).explain("extended");

        System.out.println(("============================explain(mode = \"codegen\")-展示可执行java代码==================================="));
        sparkSession.sql(sqlstr).explain("codegen");

        System.out.println(("============================explain(mode = \"formatted\")-展示格式化的物理执行计划============================="));
        sparkSession.sql(sqlstr).explain("formatted");

        //4. 关闭sparkSession
        sparkSession.close();
    }
}
