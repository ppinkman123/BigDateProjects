package com.atguigu.gmall.util;

public class MySQLUtil {
    public static String getBaseDicDDL(){
        return "CREATE TABLE base_dic (\n" +
                "  dic_code STRING,\n" +
                "  dic_name STRING,\n" +
                "  parent_code STRING,\n" +
                "  create_time STRING,\n" +
                "  operate_time STRING\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "   'table-name' = 'base_dic',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456', \n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
    }
}
