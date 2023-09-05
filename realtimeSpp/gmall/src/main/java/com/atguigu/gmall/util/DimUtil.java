package com.atguigu.gmall.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.GmallConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id){
       //获取连接
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = tableName + id;
        String redisValue = jedis.get(redisKey);
        //hbase中的维度信息用json的形式转出来会很方便
        JSONObject dimInfo = null ;
        if(redisValue == null){
            dimInfo = getDimInfo(connection, tableName, new Tuple2<>("ID", id));
            jedis.setex(redisKey,60*60*24*2,dimInfo.toJSONString());
        }else {
            dimInfo = JSONObject.parseObject(redisValue);
        }
        jedis.close();

        return dimInfo;
    }

    public static JSONObject getDimInfo(Connection connection, String tableName, Tuple2<String,String>... tuples){
        StringBuilder sql = new StringBuilder();
        sql.append("select * from")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(tableName)
                .append("where");

        for (int i = 0; i < tuples.length; i++) {
            sql.append(tuples[i].f0)
                    .append("='")
                    .append(tuples[i].f1)
                    .append("'");

            if(i < tuples.length-1){
                sql.append(" and ");
            }

        }
        System.out.println(sql.toString());
        List<JSONObject> jsonObjects = PhoenixUtil.sqlQuery(connection, sql.toString(), JSONObject.class);
        if(jsonObjects.size()>0){
            return jsonObjects.get(0);
        }else {
            return new JSONObject();
        }
    }
}
