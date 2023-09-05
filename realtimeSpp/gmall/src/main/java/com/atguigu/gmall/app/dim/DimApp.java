package com.atguigu.gmall.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.app.func.MyBroFunc;
import com.atguigu.gmall.app.func.MyPhoenixSink;
import com.atguigu.gmall.bean.TableProcess;
import com.atguigu.gmall.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DimApp {
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

        // TODO 3 读取kafka的数据
        String topic_db = "topic_db";
        String dim_app_0409 = "dim_app_0409";
        DataStreamSource<String> kafkaSource = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic_db, dim_app_0409));

        // TODO 4 过滤脏数据并转换为jsonObject

        OutputTag<String> dirty = new OutputTag<String>("dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonProcess = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type))  {
                        ctx.output(dirty, value);
                    } else {
                        out.collect(jsonObject);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                    ctx.output(dirty, value);
                }

            }
        });
//        jsonProcess.getSideOutput(dirty).print();

//       jsonProcess.print();

        // TODO 5 使用flinkCDC实时监控配置表
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .username("root")
                .password("123456")
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config")
                // 坑
                .tableList("gmall_config.table_process")
                // CDC读取数据的模式  -> 初始数据全部读一遍
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dimInfoSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "dimInfoSource");

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table_process", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = dimInfoSource.broadcast(mapStateDescriptor);

        dimInfoSource.print();
        // TODO 6 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonProcess.connect(broadcastStream);

        // TODO 7 处理连接流
        SingleOutputStreamOperator<JSONObject> tableProcessStream = connectedStream.process(new MyBroFunc(mapStateDescriptor));

//        tableProcessStream.print();
        // TODO 8 数据写出到hbase
        tableProcessStream.addSink(new MyPhoenixSink());
        // TODO 执行
        env.execute();


    }
}
