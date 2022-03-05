package com.yyj.flink;

import com.yyj.kafka.KafkaProps;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkKafkaTest {
    public static void main(String[] args) throws Exception {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaProps.BOOTSTRAP_SERVERS_CONFIG)
                .setGroupId("FlinkKafkaTest")
                .setTopics("yyj1")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                //.setBounded(OffsetsInitializer.latest())//设置消费到哪里推出程序
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");
        stream.print();

        env.execute("FlinkKafkaTest");
    }
}
