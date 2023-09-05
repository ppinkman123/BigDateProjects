package com.atguigu.gmall.util;

import com.atguigu.gmall.bean.TransientSink;
import com.atguigu.gmall.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    public static <T> SinkFunction<T> getClickHouseSink(String sql){
        return JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
            @SneakyThrows
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                Class<?> aClass = t.getClass();
                Field[] fields = aClass.getDeclaredFields();

                int offset = 0;
                for (int i = 0; i < fields.length; i++) {
                    Field field = fields[1];

                    TransientSink annotation = field.getAnnotation(TransientSink.class);
                    if(annotation == null){
                        field.setAccessible(true);

                        preparedStatement.setObject(i+1-offset,field.get(t));
                    }else {
                        offset ++;
                    }
                }
            }
        }, JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(10)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());
    }
}
