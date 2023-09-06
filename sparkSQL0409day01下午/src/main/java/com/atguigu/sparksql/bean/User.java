package com.atguigu.sparksql.bean;

import lombok.Data;

/**
 * @author yhm
 * @create 2022-07-06 15:19
 */
@Data
public class User {
    private Long age;
    private String name;

    public User() {
    }

    public User(Long age, String name) {
        this.age = age;
        this.name = name;
    }
}
