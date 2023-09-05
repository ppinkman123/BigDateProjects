package com.atguigu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class procTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

//        Table sqlQuery = tableEnvironment.sqlQuery("select PROCTIME() pt");
//
//        tableEnvironment.createTemporaryView("name",sqlQuery);
//
//        tableEnvironment.executeSql("select * from name").print();

        tableEnvironment.sqlQuery("select localtimestamp," +
                "current_timestamp," +
                "now()," +
                "PROCTIME()," +
                "current_row_timestamp()," +
                "1645406174 as TO_TIMESTAMP_LTZ(1645406174,0)")
                .execute()
                .print();


        env.execute();
    }
}
