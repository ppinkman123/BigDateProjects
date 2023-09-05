package com.atguigu.bean;

import lombok.Data;

import java.io.Serializable;

/**
 * @作者：Icarus
 * @时间：2022/7/6 13:38
 */
@Data
public class User implements Serializable {
    private String date;
    private String user_id;
    private String session_id;
    private String page_id;
    private String action_time;
    private String search_keyword;
    private String click_category_id;
    private String click_product_id;
    private String order_category_ids;
    private String order_product_ids;
    private String pay_category_ids;
    private String pay_product_ids;
    private String city_id;

    public User() {
    }

    public User(String date, String user_id, String session_id, String page_id, String action_time, String search_keyword, String click_category_id, String click_product_id, String order_category_ids, String order_product_ids, String pay_category_ids, String pay_product_ids, String city_id) {
        this.date = date;
        this.user_id = user_id;
        this.session_id = session_id;
        this.page_id = page_id;
        this.action_time = action_time;
        this.search_keyword = search_keyword;
        this.click_category_id = click_category_id;
        this.click_product_id = click_product_id;
        this.order_category_ids = order_category_ids;
        this.order_product_ids = order_product_ids;
        this.pay_category_ids = pay_category_ids;
        this.pay_product_ids = pay_product_ids;
        this.city_id = city_id;
    }


}
