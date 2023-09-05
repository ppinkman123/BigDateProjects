package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.UserLoginBean;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class login {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window";
        DataStreamSource<String> sourceStream = env.addSource(KafkaUtil.getFlinkKafkaConsumer(page_topic, groupId));

        //转json判断

        SingleOutputStreamOperator<JSONObject> jsonStreasm = sourceStream.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

//        jsonStreasm.assignTimestampsAndWatermarks(WatermarkStrategy.<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
//        .withTimestampAssigner(new SerializableTimestampAssigner<UserLoginBean>() {
//            @Override
//            public long extractTimestamp(UserLoginBean element, long recordTimestamp) {
//                return 0;
//            }
//        }),);
    }
}
