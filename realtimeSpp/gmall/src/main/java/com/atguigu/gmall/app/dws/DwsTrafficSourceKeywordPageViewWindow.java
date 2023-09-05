package com.atguigu.gmall.app.dws;

import com.atguigu.gmall.app.func.UDTFKeywordsFunc;
import com.atguigu.gmall.bean.KeywordBean;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
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

        // TODO 3 读取page页面主题数据
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_log(\n" +
                "  `common` MAP<STRING,STRING>,\n" +
                "  `page`   MAP<STRING,STRING>,\n" +
                "  `ts`    bigint,\n" +
                "  ts3  as  TO_TIMESTAMP_LTZ(ts, 3) ,\n" +
                "  WATERMARK FOR ts3 AS ts3 - INTERVAL '2' SECOND\n" +
                ")" + KafkaUtil.getKafkaDDL(page_topic,groupId));

//        tableEnv.executeSql("select * from page_log").print();
        // TODO 4 过滤页面中的关键字
        Table keywordsTable = tableEnv.sqlQuery("select\n" +
                "  `page`['item'] keywords,\n" +
                "  ts3\n" +
                "from page_log\n" +
                "where `page`['last_page_id'] = 'search'\n " +
                "and `page`['item_type'] = 'keyword'");

        tableEnv.createTemporaryView("keywordsTable",keywordsTable);
//        keywordsTable.execute().print();
        // TODO 5 拆分关键字
        tableEnv.createTemporaryFunction("SplitFunction",UDTFKeywordsFunc.class);

        Table wordTable = tableEnv.sqlQuery("select\n" +
                "  keyword,\n" +
                "  ts3\n" +
                "from keywordsTable , \n" +
                "LATERAL TABLE(SplitFunction(keywords))");

        tableEnv.createTemporaryView("wordTable",wordTable);

        // TODO 6 开窗统计关键词次数

        Table countsTable = tableEnv.sqlQuery("select\n" +
                "  DATE_FORMAT(TUMBLE_START(ts3, INTERVAL '10' second) , 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                "  DATE_FORMAT(TUMBLE_END(ts3, INTERVAL '10' second) , 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                "  'search' source,\n" +
                "  '123' err,\n" +
                "  keyword,\n" +
                "  count(*) keyword_count,\n" +
                "  UNIX_TIMESTAMP()*1000 ts\n" +
                "  from wordTable\n" +
                "  group by\n" +
                "  TUMBLE(ts3, INTERVAL '10' second),\n" +
                "  keyword");

        DataStream<KeywordBean> rowDataStream = tableEnv.toAppendStream(countsTable, KeywordBean.class);
        rowDataStream.print();
        env.execute();

    }
}
