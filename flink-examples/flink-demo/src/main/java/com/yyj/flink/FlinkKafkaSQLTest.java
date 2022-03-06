package com.yyj.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkKafkaSQLTest {

    public static void main(String[] args) throws Exception {
        //初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //建表语句
        final String createTable1 =
                "CREATE TABLE tb_user1 (\n"
                        + "  `name` STRING,\n"
                        + "  `age` INTEGER,\n"
                        + "  `topic` STRING METADATA FROM 'topic',\n"
                        + "  `partition` BIGINT METADATA VIRTUAL,\n"
                        + "  `offset` BIGINT METADATA VIRTUAL,\n"
                        + "  `timestampType` STRING METADATA FROM 'timestamp-type',\n"
                        + "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'yyj1',\n"
                        + "  'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',\n"
                        + "  'properties.group.id' = 'group1',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'format' = 'json'\n"
                        + ")";
        //创建tb_user1表
        tEnv.executeSql(createTable1);

//        final String createTable2 =
//                "CREATE TABLE tb_user2 (\n"
//                        + "  `name` STRING,\n"
//                        + "  `age` INTEGER\n"
//                        + ") WITH (\n"
//                        + "  'connector' = 'kafka',\n"
//                        + "  'topic' = 'yyj2',\n"
//                        + "  'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',\n"
//                        + "  'properties.group.id' = 'group2',\n"
//                        + "  'scan.startup.mode' = 'latest-offset',\n"
//                        + "  'format' = 'json'\n"
//                        + ")";
//        //创建tb_user2表
//        tEnv.executeSql(createTable2);
//
//        final String insert = "insert into tb_user2 select name,age from tb_user1";
//        //查询tb_user1表，插入tb_user2表
//        tEnv.executeSql(insert);
//
//        //查询tb_user2表
//        final String query = "select name,age from tb_user2";

        //查询表内容
        final String query = "select * from tb_user1";
        tEnv.toDataStream(tEnv.sqlQuery(query)).print();

        env.execute("FlinkKafkaSQLTest");
    }
}
