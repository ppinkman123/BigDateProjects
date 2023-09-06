package com.atguigu.sparksql.demo;

import lombok.Data;
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
 * @author yhm
 * @create 2022-07-08 15:04
 */
public class Test02_Top3 {
    public static void main(String[] args) {
        // 1. 创建sparkConf配置对象
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");

        // 2. 创建sparkSession连接对象
        SparkSession spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();

        // 3. 编写代码
        // 将3个表格数据join在一起
        Dataset<Row> t1DS = spark.sql("select \n" +
                "\tc.area,\n" +
                "\tc.city_name,\n" +
                "\tp.product_name\n" +
                "from\n" +
                "\tuser_visit_action u\n" +
                "join\n" +
                "\tcity_info c\n" +
                "on\n" +
                "\tu.city_id=c.city_id\n" +
                "join\n" +
                "\tproduct_info p\n" +
                "on\n" +
                "\tu.click_product_id=p.product_id");

        t1DS.createOrReplaceTempView("t1");

        spark.udf().register("cityMark",udaf(new CityMark(),Encoders.STRING()));


        // 将区域内的产品点击次数统计出来
        Dataset<Row> t2ds = spark.sql("select \n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tcityMark(city_name) mark,\n" +
                "\tcount(*) counts\n" +
                "from\t\n" +
                "\tt1\n" +
                "group by\n" +
                "\tarea,product_name");

//        t2ds.show(false);
        t2ds.createOrReplaceTempView("t2");

        // 对区域内产品点击的次数进行排序  找出区域内的top3
        spark.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tmark,\n" +
                "\trank() over (partition by area order by counts desc) rk\n" +
                "from \n" +
                "\tt2").createOrReplaceTempView("t3");

        // 使用过滤  取出区域内的top3
        spark.sql("select\n" +
                "\tarea,\n" +
                "\tproduct_name,\n" +
                "\tmark \n" +
                "from\n" +
                "\tt3\n" +
                "where \n" +
                "\trk < 4").show(50,false);

        // 4. 关闭sparkSession
        spark.close();
    }

    @Data
    public static class Buffer implements Serializable {
        private Long totalCount;
        private HashMap<String,Long> map;

        public Buffer() {
        }

        public Buffer(Long totalCount, HashMap<String, Long> map) {
            this.totalCount = totalCount;
            this.map = map;
        }
    }

    public static class CityMark extends Aggregator<String,Buffer,String>{

        @Override
        public Buffer zero() {
            return new Buffer(0L,new HashMap<String,Long>());
        }

        /**
         * 分区内的预聚合
         * @param b  map(城市,sum)
         * @param a  当前行表示的城市
         * @return
         */
        @Override
        public Buffer reduce(Buffer b, String a) {
            HashMap<String, Long> hashMap = b.getMap();
            // 如果map中已经有当前城市  次数+1
            // 如果map中没有当前城市    0+1
            hashMap.put(a, hashMap.getOrDefault(a,0L) + 1);

            b.setTotalCount(b.getTotalCount() + 1L);
            return b;
        }

        /**
         * 合并多个分区间的数据
         * @param b1    (北京,100),(上海,200)
         * @param b2    (天津,100),(上海,200)
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

        /**
         * map => {(上海,200),(北京,100),(天津,300)}
         * @param reduction
         * @return
         */
        @Override
        public String finish(Buffer reduction) {
            Long totalCount = reduction.getTotalCount();
            HashMap<String, Long> map = reduction.getMap();
            // 需要对map中的value次数进行排序
            TreeMap<Long, String> treeMap = new TreeMap<>();

            // 将map中的数据放入到treeMap中 进行排序
            map.forEach(new BiConsumer<String, Long>() {
                @Override
                public void accept(String s, Long aLong) {
                    if (treeMap.containsKey(aLong)){
                        // 如果已经有当前值
                        treeMap.put(aLong,treeMap.get(aLong) + "_" + s );
                    }else {
                        // 没有当前值
                        treeMap.put(aLong,s );
                    }
                }
            });

            ArrayList<String> resultMark = new ArrayList<>();

            Double sum = 0.0;

            // 当前没有更多的城市数据  或者  已经找到两个城市数据了  停止循环
            while(!(treeMap.size() == 0) && resultMark.size() < 2){
                String cities = treeMap.lastEntry().getValue();
                Long counts = treeMap.lastEntry().getKey();
                String[] strings = cities.split("_");
                for (String city : strings) {
                    double rate = counts.doubleValue() * 100 / totalCount;
                    sum += rate;
                    resultMark.add(city + String.format("%.2f",rate) + "%");
                }
                // 添加完成之后删除当前key
                treeMap.remove(counts);
            }

            // 拼接其他城市
            if (treeMap.size() > 0 ){
                resultMark.add("其他" + String.format("%.2f",100 - sum) + "%");
            }

            StringBuilder cityMark = new StringBuilder();
            for (String s : resultMark) {
                cityMark.append(s).append(",");
            }

            return cityMark.substring(0, cityMark.length() - 1);
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
