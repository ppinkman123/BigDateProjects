package com.atguigu;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.qBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

public class Question {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Question");
        DataStreamSource<JSONObject> sourceStream = env.addSource(new FlinkKafkaConsumer<JSONObject>("topic", new DeserializationSchema<JSONObject>() {
            @Override
            public JSONObject deserialize(byte[] message) throws IOException {
                return null;
            }

            @Override
            public boolean isEndOfStream(JSONObject nextElement) {
                return false;
            }

            @Override
            public TypeInformation<JSONObject> getProducedType() {
                return null;
            }
        }, properties));

        //过滤加转换
        KeyedStream<qBean, String> keyedStream = sourceStream.flatMap(new FlatMapFunction<JSONObject, qBean>() {
            @Override
            public void flatMap(JSONObject value, Collector<qBean> out) throws Exception {
                String logName = value.getString("log_name");
                JSONArray itemId = value.getJSONArray("item_id");
                try {
                    if ("buy".equals(logName)) {
                        for (Object id : itemId) {
                            out.collect(new qBean("buy", 1, 0,9l));
                        }
                    } else {
                        for (Object id : itemId) {
                            out.collect(new qBean("refresh", 0, 1,9l));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }).keyBy(new KeySelector<qBean, String>() {
            @Override
            public String getKey(qBean value) throws Exception {
                return value.getItemId();
            }
        });

        //开窗计算
        SingleOutputStreamOperator<qBean> reduceStream = keyedStream.assignTimestampsAndWatermarks(WatermarkStrategy.<qBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                .withTimestampAssigner(new SerializableTimestampAssigner<qBean>() {
                    @Override
                    public long extractTimestamp(qBean element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                })).windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(new ReduceFunction<qBean>() {
                    @Override
                    public qBean reduce(qBean value1, qBean value2) throws Exception {
                        value1.setBuy(value1.getBuy() + value2.getBuy());
                        value1.setRefresh(value1.getRefresh() + value2.getRefresh());
                        return value1;
                    }
                });

//        输出结果
//        reduceStream.addSink(new RichSinkFunction<qBean>() {
//            Connection connection = null;
//            MapState<String, qBean> itemCounts = null;
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//                connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall", "root", "123456");
//
//                //状态记录数值
//                itemCounts = getRuntimeContext().getMapState(new MapStateDescriptor<String, qBean>("ItemCounts", String.class, qBean.class));
//            }
//
//            @Override
//            public void invoke(qBean value, Context context) throws Exception {
//                qBean qBean = itemCounts.get(value.getItemId());
//
//                if()
//            }
//        })
    }
}
