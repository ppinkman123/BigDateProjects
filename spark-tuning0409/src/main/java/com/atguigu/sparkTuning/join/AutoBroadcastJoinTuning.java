package com.atguigu.sparkTuning.join;

import com.atguigu.sparkTuning.utils.InitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author yhm
 * @create 2022-07-07 17:53
 */
public class AutoBroadcastJoinTuning {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = InitUtil.initSparkSession(conf);

        spark.sql("use sparktuning;");
        // 3. 编写代码
        String sqlStr = "select\n" +
                "  sc.courseid,\n" +
                "  csc.courseid\n" +
                "from sale_course sc join course_shopping_cart csc\n" +
                "on sc.courseid=csc.courseid";

        spark.sql(sqlStr);

        // 生成统计信息
//        spark.sql("ANALYZE TABLE sale_course COMPUTE STATISTICS;");
        spark.sql("DESC FORMATTED sale_course;").show(100,false);

        //  BROADCASTJOIN(sc)  BROADCAST(sc)  MAPJOIN(sc)
        String sqlStr1 = "select /*+  BROADCASTJOIN(sc) */\n" +
                "  sc.courseid,\n" +
                "  csc.courseid\n" +
                "from sale_course sc join course_shopping_cart csc\n" +
                "on sc.courseid=csc.courseid";

        spark.sql(sqlStr1).explain();
        spark.sql(sqlStr1).show();

        Thread.sleep(6000000);

        // 4. 关闭sparkSession
        spark.close();
    }
}
