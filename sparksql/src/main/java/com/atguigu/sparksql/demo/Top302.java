package com.atguigu.sparksql.demo;

import lombok.Data;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import static org.apache.spark.sql.functions.udaf;

/**
 * @作者：Icarus
 * @时间：2022/7/9 11:50
 */
public class Top302 {
    public static void main(String[] args) {
       // 1. 创建sparkConf配置对象
       SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

       // 2. 创建sparkSession连接对象
       SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

       // 3. 编写代码
        Dataset<Row> t1DS = spark.sql("select\n" +
                "c.area,\n" +
                "c.city_name,\n" +
                "p.product_name\n" +
                "from\n" +
                "user_visit_action u\n" +
                "join\n" +
                "city_info c \n" +
                "on u.city_id = c.city_id\n" +
                "join\n" +
                "product_info p\n" +
                "on\n" +
                "u.click_product_id = p.product_id\n");
        t1DS.createOrReplaceTempView("t1");

        spark.udf().register("myCity",udaf(new Mycity(),Encoders.STRING()));

        Dataset<Row> t2DS = spark.sql("select\n" +
                "area,\n" +
                "product_name,\n" +
                "myCity(city_name) mcity,\n" +
                "count(*) counts\n" +
                "from\n" +
                "t1\n" +
                "group by \n" +
                "area,product_name");

        t2DS.createOrReplaceTempView("t2");

        Dataset<Row> t3DS = spark.sql("select\n" +
                "area,\n" +
                "product_name,\n" +
                "mcity,\n" +
                "rank()over(partition by area,product_name order by counts desc ) rk\n" +
                "from \n" +
                "t2");

        t3DS.createOrReplaceTempView("t3");

        spark.sql("select\n" +
                "area,\n" +
                "mcity\n" +
                "from \n" +
                "t3\n" +
                "where \n" +
                "rk <4").show(10,false);

        // 4. 关闭sparkSession
       spark.close();
    }
    @Data
    public static class Buffer implements Serializable{
        private Long totalCount;
        private HashMap<String,Long> map;

        public Buffer() {
        }

        public Buffer(Long totalCount, HashMap<String, Long> map) {
            this.totalCount = totalCount;
            this.map = map;
        }
    }
    public static class Mycity extends Aggregator<String,Buffer,String>{

        @Override
        public Buffer zero() {
            return new Buffer(0L,new HashMap<>());
        }
        //分区的计算
        @Override
        public Buffer reduce(Buffer b, String a) {
            b.setTotalCount(b.getTotalCount()+1);
            HashMap<String, Long> map = b.getMap();
            map.put(a,map.getOrDefault(a,0L)+1);//这样就可以实现累加的效果了
            return b;
        }
        //分区间计算

        /**
         * |华东|杭州17.10%,上海14.20%,青岛14.20%,其他54.49%|
         * |华东|青岛18.65%,上海16.40%,其他64.95%           |
         * |华中|长沙55.45%,武汉44.55%                      |
         * |华北|天津21.28%,郑州20.85%,其他57.87%           |
         * |东北|哈尔滨35.88%,沈阳33.59%,其他30.53%         |
         * |华东|济南18.57%,上海15.71%,其他65.71%           |
         * |西北|银川52.04%,西安47.96%                      |
         * |西北|西安63.04%,银川36.96%                      |
         * |西南|贵阳42.03%,成都30.43%,其他27.54%           |
         * |华南|厦门29.67%,深圳29.67%,其他40.67%
         * @param b1
         * @param b2
         * @return
         */
        @Override
        public Buffer merge(Buffer b1, Buffer b2) {
            b1.setTotalCount( b1.getTotalCount() + b2.getTotalCount());

            HashMap<String, Long> map1 = b1.getMap();
            HashMap<String, Long> map2 = b2.getMap();
            // 将map2中的数据放入合并到map1
            map2.forEach(new BiConsumer<String, Long>() {
                @Override
                public void accept(String s, Long aLong) {
                    map1.put(s,aLong + map1.getOrDefault(s,0L));
                }
            });

            return b1;
        }

        @Override
        public String finish(Buffer reduction) {
            TreeMap<Long, String> treeMap = new TreeMap<>();
            HashMap<String, Long> map = reduction.getMap();
            Long totalCount = reduction.getTotalCount();
            //这里是每个map都要写到tree里
            map.forEach(new BiConsumer<String, Long>() {
                @Override
                public void accept(String s, Long aLong) {
                    if(treeMap.containsKey(aLong)){
                        treeMap.put(aLong,treeMap.get(aLong)+"_"+s);
                    }else {
                        treeMap.put(aLong,s);
                    }
                }
            });

            //这里只要前两名，用循环
            ArrayList<String> result = new ArrayList<>();

            Double sum = 0.0;

            while (result.size()<2 && treeMap.size()!=0){
                String cities = treeMap.lastEntry().getValue();
                Long counts = treeMap.lastEntry().getKey();
                String[] strings = cities.split("_");
                for (String city : strings) {
                    double rate = counts.doubleValue()*100/totalCount;
                    sum += rate;
                    result.add(city + String.format("%.2f",rate) + "%");
                }
                treeMap.remove(counts);
            }

            //如果还有其他城市
            if(treeMap.size()>0){
                result.add("其他" + String.format("%.2f",100-sum)+"%");
            }

            StringBuilder builder = new StringBuilder();
            for (String s : result) {
                builder.append(s).append(",");
            }
            return builder.substring(0,builder.length()-1);
        }

        @Override
        public Encoder<Buffer> bufferEncoder() {
            return Encoders.javaSerialization(Buffer.class);
        }

        @Override
        public Encoder<String> outputEncoder() {
            return Encoders.STRING();
        }
    }
}
