package com.atguigu.gmall.app.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 获取page主题数据

        String page_topic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(page_topic, groupId));

        // TODO 4 过滤转换结构
        SingleOutputStreamOperator<JSONObject> jsonStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                out.collect(JSONObject.parseObject(value));
            }
        });

        //加时间戳和水印
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));

        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        // TODO 5 定义cep规则
        Pattern<JSONObject, JSONObject> jumpPattern = Pattern.<JSONObject>begin("begin")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                })
                .next("next")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null;
                    }
                }).within(Time.seconds(10L));

        // TODO 6 将cep规则作用于主流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, jumpPattern);

        // TODO 7 对cep规则流进行处理
        OutputTag<JSONObject> timeoutOutputTag = new OutputTag<JSONObject>("timeout", TypeInformation.of(JSONObject.class));

        SingleOutputStreamOperator<JSONObject> jumpPageStream = patternStream.flatSelect(timeoutOutputTag, new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<JSONObject> out) throws Exception {
                JSONObject begin = pattern.get("begin").get(0);
                out.collect(begin);

            }
        }, new PatternFlatSelectFunction<JSONObject, JSONObject>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<JSONObject> out) throws Exception {
                out.collect(pattern.get("begin").get(0));
            }
        });

        jumpPageStream.print("jump>>>");
        // TODO 8 读取侧输出流的超时数据
        DataStream<JSONObject> timeoutStream = jumpPageStream.getSideOutput(timeoutOutputTag);
        timeoutStream.print();




    }
}
