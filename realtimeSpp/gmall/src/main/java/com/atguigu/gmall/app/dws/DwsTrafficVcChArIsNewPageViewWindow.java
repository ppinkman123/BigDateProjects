package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TrafficPageViewBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {
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

        // TODO 3 读取3条流的数据
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_vc_ch_ar_is_new_page_view_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(page_topic, groupId));

        // 3.2 读取独立访客主题数据
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        DataStreamSource<String> uvStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId));

        // 3.3 读取跳出页面主题数据
        String jumpTopic = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> jumpStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(jumpTopic, groupId));

        // TODO 4 转换3条流  存储对应的度量值
        SingleOutputStreamOperator<TrafficPageViewBean> pageBeanStream = pageStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                String last_page_id = page.getString("last_page_id");
                long sv = 0;
                if (last_page_id == null) {
                    sv = 1;
                }
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 0L, sv, 1L, page.getLong("during_time"), 0L, jsonObject.getLong("ts"));
            }
        });

        SingleOutputStreamOperator<TrafficPageViewBean> uvBeanStream = uvStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 1L, 0L, 0L, 0L, 0L, jsonObject.getLong("ts"));
            }
        });

        SingleOutputStreamOperator<TrafficPageViewBean> jumpBeanStream = jumpStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"), common.getString("ar"), common.getString("is_new"), 0L, 0L, 0L, 0L, 1L, jsonObject.getLong("ts"));
            }
        });

        // TODO 5 合并3条流的数据
        DataStream<TrafficPageViewBean> unionStream = pageBeanStream.union(jumpBeanStream).union(uvBeanStream);

        // TODO 6 开窗10s聚合数据
        KeyedStream<TrafficPageViewBean, String> keyedStream = unionStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        })).keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean value) throws Exception {
                return value.getAr() + value.getCh() + value.getVc() + value.getIsNew();
            }
        });

        SingleOutputStreamOperator<TrafficPageViewBean> reduceStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        return value1;
                    }
                }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window,  Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean next = input.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });

        reduceStream.addSink(ClickHouseUtil.getClickHouseSink("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute(groupId);

    }
}
