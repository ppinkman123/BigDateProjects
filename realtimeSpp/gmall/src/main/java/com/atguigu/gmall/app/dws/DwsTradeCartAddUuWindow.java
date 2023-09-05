package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.CartAddUuBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
// TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从 Kafka 读取用户加购明细数据，统计当天加购独立用户数，写入 ClickHouse。

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取加购数据
        String topicName = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> sourceStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topicName, groupId));

        //转json
        SingleOutputStreamOperator<JSONObject> jsonStream = sourceStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        //keyby后直接去重
        SingleOutputStreamOperator<CartAddUuBean> beanStream = jsonStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                String uid = value.getJSONObject("common").getString("uid");

                return uid;
            }
        }).flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            ValueState<String> lastAddDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastAddDtDescriptor = new ValueStateDescriptor<String>("last_add_dt", String.class);
                lastAddDtDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

                lastAddDtState = getRuntimeContext().getState(lastAddDtDescriptor);

            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                String lastAddDt = lastAddDtState.value();
                Long ts = value.getLong("ts");
                String toDate = DateFormatUtil.toDate(ts * 1000);

                if (lastAddDt == null || !(lastAddDt.equals(toDate))) {
                    out.collect(new CartAddUuBean("", "", 1L, ts * 1000L));
                    lastAddDtState.update(toDate);
                }
            }
        });

        SingleOutputStreamOperator<CartAddUuBean> reduceStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<CartAddUuBean>() {
                    @Override
                    public long extractTimestamp(CartAddUuBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());

                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });

        reduceStream.addSink(ClickHouseUtil.getClickHouseSink("insert into t value(?,?,?,?)"));
        env.execute();
    }
}
