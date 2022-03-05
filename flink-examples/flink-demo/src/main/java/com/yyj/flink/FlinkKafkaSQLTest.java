package com.yyj.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkKafkaSQLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        final String createTable =
                "CREATE TABLE tb_user (\n"
                        + "  `name` STRING,\n"
                        + "  `age` INTEGER\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'yyj1',\n"
                        + "  'properties.bootstrap.servers' = 'localhost:9092,localhost:9093,localhost:9094',\n"
                        + "  'properties.group.id' = 'group1',\n"
                        + "  'scan.startup.mode' = 'latest-offset',\n"
                        + "  'format' = 'json'\n"
                        + ")";
        tEnv.executeSql(createTable);

        String query = "select * from tb_user";
        tEnv.toDataStream(tEnv.sqlQuery(query)).print();

        env.execute("FlinkKafkaSQLTest");
    }
}
