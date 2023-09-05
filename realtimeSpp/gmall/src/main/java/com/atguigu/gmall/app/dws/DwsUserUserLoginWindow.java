package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.UserLoginBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow {
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

        // TODO 3 读取页面数据
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(page_topic, groupId));

        // TODO 4 过滤每个会话登录数据的第一条 同时转换结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                String lastPageId = page.getString("last_page_id");
                if (common.getString("uid") != null && (lastPageId == null || "login".equals(lastPageId))) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5 根据uid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
            }
        });

        // TODO 6 通过uid进行去重得到独立用户和7天回流用户
        SingleOutputStreamOperator<UserLoginBean> beanStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

            ValueState<String> lastLoginDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtStateDescriptor = new ValueStateDescriptor<String>("last_login_dt", String.class);
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtStateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                String lastLoginDt = lastLoginDtState.value();
                Long ts = value.getLong("ts");
                String loginDt = DateFormatUtil.toDate(ts);
                long uuCt = 0;
                long backCt = 0;
                if (lastLoginDt == null || !lastLoginDt.equals(loginDt)) {
                    uuCt = 1;
                    if (lastLoginDt != null && (ts - DateFormatUtil.toTs(lastLoginDt) >= 1000L * 60 * 60 * 24 * 7)) {
                        backCt = 1;
                    }

                    lastLoginDtState.update(loginDt);
                }
                out.collect(new UserLoginBean("", "", backCt, uuCt, ts));
            }
        });

        // TODO 7 开窗聚合

        SingleOutputStreamOperator<UserLoginBean> reduceStream = beanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });

        reduceStream.addSink(ClickHouseUtil.getClickHouseSink("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        env.execute();
    }
}
