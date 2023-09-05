package com.atguigu.wordcountdemo.join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean, NullWritable> {

        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //集合比较特殊，只能便利一遍，遍历时，只换集合中这个元素的属性值，不换地址值 。


        for (TableBean value : values) {
            if("order".equals(value.getFlag())){
               TableBean tmp = new TableBean();
                try {
                    BeanUtils.copyProperties(tmp,value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmp);
            }else {                                    //商品表
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        //遍历集合orderBeans,替换掉每个orderBean的pid为pname,然后写出
        for (TableBean orderBean : orderBeans) {
                orderBean.setPname(pdBean.getPname());
                context.write(orderBean,NullWritable.get());
        }
    }
}


