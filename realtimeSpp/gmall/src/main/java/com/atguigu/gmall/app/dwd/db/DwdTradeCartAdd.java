package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.common.GmallConfig;
import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.MySQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd {
    public static void main(String[] args) {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 使用DDL语句读取kafka数据
        String topicName = "topic_db";
        String groupId = "dwd_trade_cart_add";
        tableEnv.executeSql("CREATE TABLE topic_db(\n" +
                "    `database` STRING,\n" +
                "    `table` STRING,\n" +
                "    `type` STRING,\n" +
                "    `ts` bigint,\n" +
                "    `data` MAP<STRING,STRING>,\n" +
                "    `old` MAP<STRING,STRING>,\n" +
                "    `pt` as proctime()\n" +
                ") WITH(\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = '"+ topicName + "',\n" +
                "    'properties.bootstrap.servers' = '" + GmallConfig.BOOTSTRAP_SERVER + "',\n" +
                "    'properties.group.id' = '" + groupId + "',\n" +
                "    'scan.startup.mode' = 'group-offsets',\n" +
                "    'format' = 'json'\n" +
                ")");
//        tableEnv.sqlQuery("select * from topic_db").execute().print();


        // TODO 4 过滤出加购的数据
        Table cartAddTable = tableEnv.sqlQuery("select\n" +
                "`data`[`id`] id,  \n" +
                "`data`[`user_id`] user_id,  \n" +
                "`data`[`sku_id`] sku_id,  \n" +
                "`data`[`cart_price`] cart_price,  \n" +
                "`data`[`sku_num`] sku_num,  \n" +
                "`data`[`sku_name`] sku_name,  \n" +
                "`data`[`is_checked`] is_checked,  \n" +
                "`data`[`create_time`] create_time,  \n" +
                "`data`[`operate_time`] operate_time,  \n" +
                "`data`[`is_ordered`] is_ordered,  \n" +
                "`data`[`order_time`] order_time,  \n" +
                "`data`[`source_type`] source_type,  \n" +
                "`data`[`source_id`] source_id,\n" +
                "`ts`,\n" +
                "`pt`\n" +
                "from topic_db\n" +
                "where `table`='cart_info'\n" +
                "and(('type'='insert') or\n" +
                "('type'='update' and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))");

        //TODO 5 使用mysql的DDL读取base_dic数据
        tableEnv.executeSql(MySQLUtil.getBaseDicDDL());

        // TODO 6 使用lookup join进行关联

        tableEnv.createTemporaryView("cart_add",cartAddTable);
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "  c.id,\n" +
                "  c.user_id,\n" +
                "  c.sku_id,\n" +
                "  c.cart_price,\n" +
                "  c.sku_num,\n" +
                "  c.sku_name,\n" +
                "  c.is_checked,\n" +
                "  c.create_time,\n" +
                "  c.operate_time,\n" +
                "  c.is_ordered,\n" +
                "  c.order_time,\n" +
                "  b.dic_name source_type,\n" +
                "  c.source_id,\n" +
                "  `ts`,\n" +
                "  `pt`\n" +
                "from cart_add c\n" +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt as b\n" +
                "on on c.source_type=b.dic_code");

        tableEnv.createTemporaryView("cart_add_dic_info",resultTable);

        // TODO 7 写出数据到新的kafka主题

        tableEnv.executeSql("CREATE TABLE dwd_trade_cart_add (\n" +
                "  id STRING,\n" +
                "  user_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  cart_price STRING,\n" +
                "  sku_num STRING,\n" +
                "  sku_name STRING,\n" +
                "  is_checked STRING,\n" +
                "  create_time STRING,\n" +
                "  operate_time STRING,\n" +
                "  is_ordered STRING,\n" +
                "  order_time STRING,\n" +
                "  source_type STRING,\n" +
                "  source_id STRING,\n" +
                "  ts BIGINT,\n" +
                "  pt TIMESTAMP_LTZ(3)\n" +
                ") " + KafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        tableEnv.executeSql("insert into dwd_trade_cart_add select * from cart_add_dic_info");

    }
}
