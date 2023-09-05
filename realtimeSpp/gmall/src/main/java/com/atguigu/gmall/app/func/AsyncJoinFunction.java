package com.atguigu.gmall.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DimUtil;
import com.atguigu.gmall.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class AsyncJoinFunction <T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    DruidDataSource dataSource = null;
    ThreadPoolExecutor poolExecutor = null;
    String dimTableName=null;

    public AsyncJoinFunction(String dimTableName) {
        this.dimTableName = dimTableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidPhoenixDSUtil.getDataSource();
         poolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        poolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                DruidPooledConnection connection = null;
                try {
                    connection = dataSource.getConnection();
                    JSONObject dimSkuInfo = DimUtil.getDimInfo(connection, dimTableName, getKey(input));
                    join(input,dimSkuInfo);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    }
                }
                resultFuture.complete(Collections.singleton(input));
            }
        });
    }

    // 如果超时执行这里的代码同时kill掉上面的线程
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 如果超时关联不上  就不再关联
        resultFuture.complete(Collections.singleton(input));
    }
}
