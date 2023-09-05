package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.MySQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderPreProcess {
    public static void main(String[] args) {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置存货时间 不能让数据长时间保存在内存中
       tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(905l));

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取kafka的topic_db原始数据
        String topicName = "topic_db";
        String groupId = "dwd_trade_order_pre_process";
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` bigint,\n" +
                "  `data` MAP<STRING,STRING>,\n" +
                "  `old` MAP<STRING,STRING>,\n" +
                "  `pt` as proctime()\n" +
                ")" + KafkaUtil.getKafkaDDL(topicName,groupId));

        //TODO 4 过滤出需要的4张表格数据
        //4.1过滤出订单详情表
        Table orderDetailTable = tableEnv.sqlQuery("select\n" +
                "  `data`[`id`] id,\n" +
                "  `data`[`order_id`] order_id,\n" +
                "  `data`[`sku_id`] sku_id,\n" +
                "  `data`[`sku_name`] sku_name,\n" +
                "  `data`[`order_price`] order_price,\n" +
                "  `data`[`sku_num`] sku_num,\n" +
                "  `data`[`create_time`] create_time,\n" +
                "  `data`[`source_type`] source_type,\n" +
                "  `data`[`source_id`] source_id,\n" +
                "  cast(cast(`data`['sku_num'] as decimal(16,2)) * cast(`data`['order_price'] as decimal(16,2)) as STRING) split_original_amount,\n" +
                "  `data`[`split_activity_amount`] split_activity_amount,\n" +
                "  `data`[`split_coupon_amount`] split_coupon_amount,\n" +
                "  `ts`,\n" +
                "  `pt` \n" +
                "from topic_db\n" +
                "where `table`=`order_detail`\n" +
                "and `type`=`insert`");

        tableEnv.createTemporaryView("order_detail",orderDetailTable);

        // 4.2 过滤订单表
        Table orderInfo = tableEnv.sqlQuery("select  \n" +
                "  data['id'] id,\n" +
                "  data['user_id'] user_id,\n" +
                "  data['province_id'] province_id,\n" +
                "  data['operate_time'] operate_time,\n" +
                "  data['order_status'] order_status,\n" +
                "  type ,\n" +
                "  `ts` ,\n" +
                "  `pt`\n" +
                "from topic_db\n" +
                "where `table`='order_info'");

        tableEnv.createTemporaryView("order_info",orderInfo);

        // 4.3 过滤活动表
        Table detailActivity = tableEnv.sqlQuery("select \n" +
                "  data['order_detail_id'] order_detail_id,\n" +
                "  data['activity_id'] activity_id,\n" +
                "  data['activity_rule_id'] activity_rule_id,\n" +
                "  `ts` ,\n" +
                "  `pt`\n" +
                "from topic_db\n" +
                "where `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity",detailActivity);

        // 4.4 过滤优惠券表
        Table detailCoupon = tableEnv.sqlQuery("select \n" +
                "  data['order_detail_id'] order_detail_id,\n" +
                "  data['coupon_id'] coupon_id,\n" +
                "  `ts` ,\n" +
                "  `pt`\n" +
                "from topic_db\n" +
                "where `table`='order_detail_coupon'\n" +
                "and `type`='insert'");

        tableEnv.createTemporaryView("order_detail_coupon",detailCoupon);

        // TODO 5 读取base_dic数据
        tableEnv.executeSql(MySQLUtil.getBaseDicDDL());

        Table orderDetailFullTable = tableEnv.sqlQuery("select \n" +
                "  od.id ,\n" +
                "  od.order_id ,\n" +
                "  od.sku_id ,\n" +
                "  od.sku_name ,\n" +
                "  od.order_price ,\n" +
                "  od.sku_num ,\n" +
                "  od.create_time ,\n" +
                "  bd.dic_name source_type ,\n" +
                "  od.source_id ,\n" +
                "  od.split_original_amount ,\n" +
                "  od.split_total_amount ,\n" +
                "  od.split_activity_amount ,\n" +
                "  od.split_coupon_amount ,\n" +
                "  od.ts od_ts,\n" +
                "  od.pt od_pt ,\n" +
                "  oi.user_id ,\n" +
                "  oi.province_id ,\n" +
                "  oi.operate_time ,\n" +
                "  oi.order_status ,\n" +
                "  oi.ts oi_ts ,\n" +
                "  oi.pt oi_pt ,\n" +
                "  act.activity_id ,\n" +
                "  act.activity_rule_id ,\n" +
                "  cou.coupon_id, \n" +
                "  oi.type \n" +
                "from order_detail  od \n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id\n" +
                "left join order_detail_activity act\n" +
                "on od.id=act.order_detail_id\n" +
                "left join order_detail_coupon cou\n" +
                "on od.id=cou.order_detail_id\n" +
                "join base_dic FOR SYSTEM_TIME AS OF od.pt as bd\n" +
                "on od.source_type=bd.dic_code");
        tableEnv.createTemporaryView("order_detail_full",orderDetailFullTable);

        // TODO 7 写出到kafka中
        String targetTopicName = "dwd_trade_order_pre_process";
        tableEnv.executeSql("create table kafka_order_detail(\n" +
                "  id STRING ,\n" +
                "  order_id STRING ,\n" +
                "  sku_id STRING ,\n" +
                "  sku_name STRING ,\n" +
                "  order_price STRING ,\n" +
                "  sku_num STRING ,\n" +
                "  create_time STRING ,\n" +
                "  source_type STRING ,\n" +
                "  source_id STRING ,\n" +
                "  split_original_amount STRING ,\n" +
                "  split_total_amount STRING ,\n" +
                "  split_activity_amount STRING ,\n" +
                "  split_coupon_amount STRING ,\n" +
                "  od_ts BIGINT ,\n" +
                "  od_pt TIMESTAMP_LTZ(3) ,\n" +
                "  user_id STRING ,\n" +
                "  province_id STRING ,\n" +
                "  operate_time STRING ,\n" +
                "  order_status STRING ,\n" +
                "  oi_ts BIGINT ,\n" +
                "  oi_pt TIMESTAMP_LTZ(3) ,\n" +
                "  activity_id STRING ,\n" +
                "  activity_rule_id STRING ,\n" +
                "  coupon_id STRING," +
                "  type STRING,  \n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n \n" +
                ")" + KafkaUtil.getUpsertKafkaSinkDDL(targetTopicName));

        tableEnv.executeSql("insert into kafka_order_detail select * from order_detail_full");


    }
}
