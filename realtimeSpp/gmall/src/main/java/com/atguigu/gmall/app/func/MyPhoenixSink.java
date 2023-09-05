package com.atguigu.gmall.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.GmallConfig;
import com.atguigu.gmall.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Collection;
import java.util.Set;

public class MyPhoenixSink extends RichSinkFunction<JSONObject> {
    private DruidDataSource dataSource ;

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidPhoenixDSUtil.getDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        // upsert into t columns(col1,col2,col3) values(v1,v2,v3)
        DruidPooledConnection connection = null;
        try {
            connection = dataSource.getConnection();
            String sinkTable = jsonObject.getString("sink_table");
            jsonObject.remove("sink_table");

            StringBuilder stringBuilder = new StringBuilder();

            Set<String> strings = jsonObject.keySet();
            Collection<Object> values = jsonObject.values();

            stringBuilder.append("upsert into ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append(" (")
                    .append(StringUtils.join(strings,","))
                    .append(") values( '")
                    .append(StringUtils.join(values,"','"))
                    .append("')");

            System.out.println(stringBuilder.toString());
            PhoenixUtil.sqlExecute(connection,stringBuilder.toString());
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("写入数据到phoenix错误");
        }finally {
            if (connection!=null){
                try {
                    connection.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}
