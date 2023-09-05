package com.atguigu.gmall.app.dwd.log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
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
        // TODO 3 读取topic_log的数据

        String groupId = "base_log_app_0409d";
        String topicName = "topic_log";
        DataStreamSource<String> logStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topicName, groupId));

//        logStream.print();
        // TODO 4 过滤脏数据转换结构
        SingleOutputStreamOperator<JSONObject> jsonStream = logStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // TODO 5 新旧访客修复
        /*
        (1)	新旧访客标记修复 -> 需要使用状态记录  记录第一次登录的日期yyyy-MM-dd
            如果发送过来is_new为0 ->
            判断状态是否为空 -> 如果为空 记录状态为昨天  -> 如果不为空
            如果发送过来is_new为1 ->
            判断状态为空 -> 写入今天日期为状态
                不为空 判断状态日期和今天是否为同一天 -> 同一天不用操作 -> 不是同一天把is_new改为0
         */

        KeyedStream<JSONObject, String> keyedStream = jsonStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> fixStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstVisitDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_visit_dt", String.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                //是毫秒
                Long ts = value.getLong("ts");
                String firstVisitDt = firstVisitDtState.value();
                String visitDt = DateFormatUtil.toDate(ts);

                String isNew = value.getJSONObject("common").getString("is_new");

                if ("0".equals(isNew)) {
                    if (firstVisitDt == null) {
                        firstVisitDtState.update(DateFormatUtil.toDate(ts - 1000L * 60 * 60 * 24));
                    }
                } else {
                    // 发送的是1
                    if (firstVisitDt == null) {
                        firstVisitDtState.update(visitDt);
                    } else if (!firstVisitDt.equals(visitDt)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }
                out.collect(value);
            }
        });

        // TODO 6 筛选过滤不同的数据
        // TODO 7 将不同的数据写入到不同的侧输出流
        OutputTag<String> actionOutPutTag = new OutputTag<>("action", TypeInformation.of(String.class));
        OutputTag<String> displayOutPutTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> startOutPutTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> errorOutPutTag = new OutputTag<>("error", TypeInformation.of(String.class));


        SingleOutputStreamOperator<String> processStream = fixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    ctx.output(errorOutPutTag, err.toString());
                    value.remove("err");
                }

                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    //这里是启动日志
                    ctx.output(startOutPutTag, value.toJSONString());
                } else {
                    //这里是行为日志
                    JSONObject common = value.getJSONObject("common");
                    Long ts = value.getLong("ts");
                    JSONObject page = value.getJSONObject("page");

                    //处理动作日志
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page", page);
                            ctx.output(actionOutPutTag, action.toJSONString());
                        }
                        value.remove("actions");
                    }

                    //处理曝光日志
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            ctx.output(displayOutPutTag, display.toJSONString());
                        }
                        value.remove("displays");
                    }

                    out.collect(value.toString());
                }
            }
        });


        DataStream<String> startStream = processStream.getSideOutput(startOutPutTag);
        DataStream<String> errorStream = processStream.getSideOutput(errorOutPutTag);
        DataStream<String> displayStream = processStream.getSideOutput(displayOutPutTag);
        DataStream<String> actionStream = processStream.getSideOutput(actionOutPutTag);
//        startStream.print("start>>");
//        errorStream.print("error>>");
//        displayStream.print("display>>");
//        actionStream.print("action>>");
        processStream.print("page>>>");

        processStream.addSink(KafkaUtil.getFlinkKafkaProducer(topicName));

        // TODO 10 执行任务
        env.execute(groupId);
    }
}
