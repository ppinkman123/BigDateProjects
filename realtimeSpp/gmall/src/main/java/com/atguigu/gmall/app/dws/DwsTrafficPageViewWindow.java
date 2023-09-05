package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import javafx.scene.chart.ValueAxis;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficPageViewWindow {
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

        // TODO 3 读取page页面主题数据
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window1";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(page_topic, groupId));

        // TODO 4 转换结构加过滤
        //此处给数据分流，没有办法转成bean，所以用json格式的来做判断
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject page = jsonObject.getJSONObject("page");
                if ("home".equals(page.getString("page_id")) || "good_detail".equals(page.getString("page_id"))) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5 根据mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });



        // TODO 6 根据mid去重得到独立访客 转换为javaBean
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

            ValueState<String> lastHomeVisitState = null;
            ValueState<String> lastDetailVisitState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastHomeVisitDescriptor = new ValueStateDescriptor<>("last_home_visit", String.class);
                lastHomeVisitDescriptor.enableTimeToLive(StateTtlConfig
                        .newBuilder(Time.days(1l))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());

                lastHomeVisitState = getRuntimeContext().getState(lastHomeVisitDescriptor);

                ValueStateDescriptor<String> lastDetailVisitDescriptor = new ValueStateDescriptor<>("last_detail_visit", String.class);
                lastDetailVisitDescriptor.enableTimeToLive(StateTtlConfig
                        .newBuilder(Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastDetailVisitState = getRuntimeContext().getState(lastDetailVisitDescriptor);
            }


            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                Long ts = value.getLong("ts");
                String visitDt = DateFormatUtil.toDate(ts);
                long homeUvCt = 0;
                long goodDetailUvCt = 0;
                if ("home".equals(pageId)) {
                    String lastHomeVisit = lastHomeVisitState.value();
                    if (lastHomeVisit == null || !lastHomeVisit.equals(visitDt)) {
                        homeUvCt = 1;
                        lastHomeVisitState.update(visitDt);
                    } else {
                        String lastDetailVisit = lastDetailVisitState.value();
                        if (lastDetailVisit == null || !lastDetailVisit.equals(visitDt)) {
                            goodDetailUvCt = 1;
                            lastDetailVisitState.update(visitDt);
                        }
                    }
                }
                out.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, goodDetailUvCt, ts));

            }
        });
//        beanStream.print();
//
//        env.execute();

        // TODO 7 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = beanStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean next = values.iterator().next();
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setTs(System.currentTimeMillis());
                        out.collect(next);
                    }
                });
        reduceStream.print("reduce>>>");
        // TODO 8 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getClickHouseSink("insert into dws_traffic_page_view_window valuse(?,?,?,?,?"));

        env.execute();
    }
}
