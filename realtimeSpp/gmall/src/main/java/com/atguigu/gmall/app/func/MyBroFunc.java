package com.atguigu.gmall.app.func;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TableProcess;
import com.atguigu.gmall.common.GmallConfig;
import com.atguigu.gmall.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import scala.Predef;

import java.sql.SQLException;
import java.util.*;

public class MyBroFunc extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private DruidDataSource druidDataSource ;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public MyBroFunc(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidPhoenixDSUtil.getDataSource();
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);


        JSONObject jsonObject = JSON.parseObject(value);
        String op = jsonObject.getString("op");
        if("d".equals(op)){
            JSONObject before = jsonObject.getJSONObject("before");
            String sourceTable = before.getString("source_table");
            broadcastState.remove(sourceTable);
        }else {
            String after = jsonObject.getString("after");
            TableProcess tableProcess = JSONObject.parseObject(after, TableProcess.class);
            broadcastState.put(tableProcess.getSourceTable(),tableProcess);

            //创建表格
            String sinkTable = tableProcess.getSinkTable();
            String sinkColumns = tableProcess.getSinkColumns();
            String sinkPk = tableProcess.getSinkPk();
            String sinkExtend = tableProcess.getSinkExtend();


            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);


        }


    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        DruidPooledConnection connection = null ;
        if(sinkPk == null){
            sinkPk = "id";
        }

        if(sinkExtend == null){
            sinkExtend = "";
        }

        String[] cols = sinkColumns.split(",");

        StringBuilder createSql = new StringBuilder();
        createSql.append("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append(" ( ");

        for (int i = 0; i < cols.length; i++) {
            if(cols[i].equals(sinkPk)){
                createSql.append(cols[i])
                        .append(" varchar not null primary key");
            }else {
                createSql.append(cols[i])
                        .append(" varchar ");
            }

            if(i<cols.length -1){
                createSql.append(",");
            }
        }
        createSql.append(")")
                .append(sinkExtend);

        System.out.printf(createSql.toString());

        try {
            connection = druidDataSource.getConnection();
            PhoenixUtil.sqlExecute(connection,createSql.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
//        System.out.println(value);
        // TODO 1  读取对应表格的配置状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String tableName = value.getString("table");
        TableProcess tableProcess = broadcastState.get(tableName);
        if(tableProcess != null){
        // TODO 2 根据状态中的输出列过滤字段
            JSONObject data = value.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumns(sinkColumns,data);
            data.put("sink_table",tableProcess.getSinkTable());
            // TODO 3 添加sinkTable写出
            out.collect(data);

        }

    }

    private void filterColumns(String sinkColumns, JSONObject data) {
        List<String> cols = Arrays.asList(sinkColumns.split(","));
        data.entrySet().removeIf(next -> !cols.contains(next.getKey()));
    }



}
