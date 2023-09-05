package com.atguigu;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class CEPL {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 9999);




        //cep必须要有水位线
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = hadoop102.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return System.currentTimeMillis();
                    }
                }));

        //定义规则
        Pattern<String, String> pattern = Pattern.<String>begin("begin")
                .where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String value, Context<String> ctx) throws Exception {
                        return value.length() > 2;
                    }
                }).next("next")
                .where(new IterativeCondition<String>() {
                    @Override
                    public boolean filter(String value, Context<String> ctx) throws Exception {
                        return value.length() > 3;
                    }
                }).within(Time.seconds(5L));

        //合并成规则流
        PatternStream<String> patternStream = CEP.pattern(stringSingleOutputStreamOperator, pattern);

        //超时数据进入的侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("timeout"){};
        //后面定义一个函数，如何处理这些数据，，数据怎么处理
        SingleOutputStreamOperator<String> outputStreamOperator = patternStream.flatSelect(outputTag,
                //什么样的数据会写到超时数据里面
                new PatternFlatTimeoutFunction<String, String>() {
                    @Override
                    public void timeout(Map<String, List<String>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        System.out.println("out---"+pattern);
                        //pattern是自己定义的规则，可以有很多个，所以要写名字
                        List<String> begin = pattern.get("begin");
                        //get(0)是因为cep可以设置times，循环多次，每次循环相同名字的都会放到这里
                        out.collect(begin.get(0));
                    }
                },
                //什么数据写到主流里面
                new PatternFlatSelectFunction<String, String>() {
                    @Override
                    public void flatSelect(Map<String, List<String>> pattern, Collector<String> out) throws Exception {
                        System.out.println("sel---"+ pattern);

                        List<String> begin = pattern.get("begin");
                        List<String> next = pattern.get("next");

                        out.collect(begin.get(0) + ":" + next.get(0));
                    }
                });

        DataStream<String> sideOutput = outputStreamOperator.getSideOutput(outputTag);

        sideOutput.print("timeout>>>");

        outputStreamOperator.print("word>>>");

        env.execute();
    }
}
