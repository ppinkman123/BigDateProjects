package com.atguigu.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * @作者：Icarus
 * @时间：2022/7/6 13:58
 */
@Data
public class Bean implements Serializable {
    private String categoryId;
    private Long clickCount;
    private Long orderCount;
    private Long payCount;

    public Bean() {
    }

    public Bean(String categoryId, Long clickCount, Long orderCount, Long payCount) {
        this.categoryId = categoryId;
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }
}