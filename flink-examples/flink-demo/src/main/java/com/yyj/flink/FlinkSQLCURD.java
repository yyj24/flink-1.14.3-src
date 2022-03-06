package com.yyj.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLCURD {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("SHOW CATALOGS").print();
        tEnv.executeSql("SHOW DATABASES").print();
        tEnv.executeSql("SHOW tables").print();
    }
}
