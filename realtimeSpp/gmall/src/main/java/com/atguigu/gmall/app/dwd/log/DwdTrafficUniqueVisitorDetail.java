package com.atguigu.gmall.app.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.mortbay.util.ajax.JSON;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
*        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取page主题数据
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_unique_visitor_detail";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(page_topic, groupId));

        // TODO 4 转换和过滤数据 过滤出每个会话的第一条数据
        SingleOutputStreamOperator<JSONObject> sessionPageStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);

                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                //能减少大量的数据
                if (lastPageId == null) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5 根据设备ID分组
        KeyedStream<JSONObject, String> keyedStream = sessionPageStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        // TODO 6 根据状态日期去重
        SingleOutputStreamOperator<JSONObject> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<String> lastVisitDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> lastVisitDtDescriptor = new ValueStateDescriptor<>("last_visit_dt", String.class);

                lastVisitDtDescriptor.enableTimeToLive(StateTtlConfig
                        .newBuilder(Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

                lastVisitDtState = getRuntimeContext().getState(lastVisitDtDescriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                String lastVisitDt = lastVisitDtState.value();
                String visitDt = DateFormatUtil.toDate(value.getLong("ts"));

                if (lastVisitDt == null || !lastVisitDt.equals(visitDt)) {
                    lastVisitDtState.update(visitDt);

                    out.collect(value);
                }
            }
        });

        // TODO 7 过滤出的独立访客数据再写会kafka
        //这一步的目的是json转string

        processStream.map(new MapFunction<JSONObject,String>() {
            @Override
            public String map(JSONObject value) throws Exception {
                return value.toString();
            }
        });

    }
}
