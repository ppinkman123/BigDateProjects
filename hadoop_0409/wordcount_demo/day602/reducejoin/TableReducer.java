package com.atguigu.hadoop.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author clh
 * @create 2022-06-02-9:23
 */
public class TableReducer  extends Reducer<Text,TableBean,TableBean, NullWritable> {
    private TableBean pdBean=new TableBean();
    private ArrayList<TableBean>  orderList=new ArrayList<>();
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
         orderList.clear();
        //01   ([],[],[]) //必须拿商品表的数据 来替换订单的数据
        //集合比较特殊 只能遍历一次 并且遍历时 只换集合中这个元素的属性值 不换地址值
        //需要将数据先确认好 并且分开
        //收集起来
        for (TableBean value : values) {
           if ("order".equals(value.getFlag())){
               //一定是订单数据   //有多个
               TableBean tmpBean = new TableBean();
               //tmpBean.setId(value.getId());
               //tmpBean.setPid(value.getPid());
               //tmpBean.setAmount(value.getAmount());
               //tmpBean.setPname(value.getPname());
               //tmpBean.setFlag(value.getFlag());
               try {
                   BeanUtils.copyProperties(tmpBean,value);
               } catch (Exception e) {
                   e.printStackTrace();
               }
               orderList.add(tmpBean);
           }else {
               //一定是商品数据  //一定只有一个
               try {
                   BeanUtils.copyProperties(pdBean,value);
               } catch (Exception e) {
                   e.printStackTrace();
               }
           }
        }
        //遍历orderList
        for (TableBean tableBean : orderList) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean,NullWritable.get());
        }
    }
}
