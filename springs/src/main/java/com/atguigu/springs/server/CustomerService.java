package com.atguigu.springs.server;

import com.atguigu.springs.bean.Customer;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.ibatis.annotations.Param;

/**
 * @作者：Icarus
 * @时间：2022/8/21 18:31
 */
public interface CustomerService extends IService<Customer> {

    public Customer getCustomer(@Param("id") String id);

    public void saveCustomer(Customer customer);

    public Customer getCustomerById(String id);
}
