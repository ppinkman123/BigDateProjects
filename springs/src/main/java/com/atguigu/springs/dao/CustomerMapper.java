package com.atguigu.springs.dao;

import com.atguigu.springs.bean.Customer;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.dynamic.datasource.annotation.DS;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @作者：Icarus
 * @时间：2022/8/21 18:58
 */

@Mapper
@DS("mysql0409")
public interface CustomerMapper extends BaseMapper<Customer> {

    @Select("select name from customer where id=#{id}")
    public Customer getCustomer(@Param("id") String id);

    public void saveCustomer(Customer customer);
}
