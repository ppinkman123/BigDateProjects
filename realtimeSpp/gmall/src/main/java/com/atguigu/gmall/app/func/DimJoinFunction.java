package com.atguigu.gmall.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    public abstract String getKey(T input);
    public abstract void join(T input, JSONObject dimSkuInfo);
}