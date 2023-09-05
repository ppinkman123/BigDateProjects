package com.atguigu.springs.controller;


import com.atguigu.springs.bean.Customer;
import com.atguigu.springs.server.CustomerService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @作者：Icarus
 * @时间：2022/8/20 19:38
 */

@RestController
public class CustomerController {
    //自动找到接口的实现
    @Autowired
    CustomerService customerService;

    @RequestMapping("customer")
    public String getCustomerByName(@RequestParam("name") String na){
        return "customer name :" +na;
    }

    @RequestMapping("customerById/{id}")
    public Customer getCustomerById(@PathVariable("id") String id){
       // return customerService.getCustomerById(id);

        return customerService.getOne(new QueryWrapper<Customer>().eq("id",id));
    }

    @PostMapping("customer")
    public void saveCustomer(@RequestBody Customer customer){
//        System.out.println("保存"+customer);
        //customerService.saveCustomer(customer);
        customerService.save(customer);
    }


}
