package com.atguigu.springs.server.impl;

import com.atguigu.springs.bean.Customer;
import com.atguigu.springs.dao.CustomerMapper;
import com.atguigu.springs.server.CustomerService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @作者：Icarus
 * @时间：2022/8/21 18:34
 */

@Service
public class CustomerServiceImpl extends ServiceImpl<CustomerMapper, Customer> implements CustomerService {

//    @Autowired
//    CustomerMapper customerMapper;

    @Override
    public Customer getCustomer(String id) {
        return null;
    }

    @Override
    public void saveCustomer(Customer customer) {
        System.out.println("服务层");
    }

    @Override
    public Customer getCustomerById(String id) {
        return this.baseMapper.selectById(id);
    }
}
