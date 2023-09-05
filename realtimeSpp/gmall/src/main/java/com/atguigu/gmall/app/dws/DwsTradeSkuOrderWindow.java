package com.atguigu.gmall.app.dws;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TradeSkuOrderBean;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.PhoenixUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.time.Duration;

public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
       //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topicName = "dwd_trade_order_detail";
        String gtoupId = "DwsTradeSkuOrderWindow";
        DataStreamSource<String> dataSource = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topicName, gtoupId));

        //过滤脏数据
        KeyedStream<JSONObject, String> keyedStream = dataSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String sourceType = jsonObject.getString("source_type");
                String userId = jsonObject.getString("user_id");
                if (userId != null || sourceType != null) {
                    out.collect(jsonObject);
                }
            }
        }).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });
        
        //去除left join 里的脏数据

        SingleOutputStreamOperator<JSONObject> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> valueState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<JSONObject>("left_join", JSONObject.class);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject stateValue = valueState.value();

                if (stateValue == null) {
                    valueState.update(value);
                    ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                } else {
                    Long oldRt = stateValue.getLong("rt");
                    Long rt = value.getLong("rt");
                    if (rt > oldRt) {
                        valueState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                out.collect(valueState.value());
                valueState.clear();
            }
        });

        //转成bean
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = processStream.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject value) throws Exception {

                return TradeSkuOrderBean.builder()
                        .skuId(value.getString("sku_id"))
                        .skuName(value.getString("sku_name"))
                        .orderAmount(Double.valueOf(value.getString("split_total_amount")))
                        .originalAmount(Double.valueOf(value.getString("split_original_amount")))
                        .activityAmount(Double.valueOf(value.getString("split_activity_amount") == null ? "0" : value.getString("split_activity_amount")))
                        .couponAmount(Double.valueOf(value.getString("split_coupon_amount") == null ? "0" : value.getString("split_coupon_amount")))
                        .ts(value.getLong("rt") * 1000L)
                        .build();

            }
        });

        //开窗计算
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeSkuOrderBean>() {
                    @Override
                    public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })).keyBy(new KeySelector<TradeSkuOrderBean, String>() {
            @Override
            public String getKey(TradeSkuOrderBean value) throws Exception {
                return value.getSkuId();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {

                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean value = input.iterator().next();
                        value.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        out.collect(value);
                    }
                });

        //关联dim表
        reduceStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

            DruidDataSource hbaseSource = null;
            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseSource = DruidPhoenixDSUtil.getDataSource();
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {
                DruidPooledConnection connection = null;
                try {
                    //获取一个连接
                    connection = hbaseSource.getConnection();
                    //拼出查询语句
                    String sql = "select * from xxx where and xxx" ;
                    //进行查询
                    PreparedStatement prepareStatement = connection.prepareStatement(sql);

                    prepareStatement.executeQuery()

                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        })


    }
}
