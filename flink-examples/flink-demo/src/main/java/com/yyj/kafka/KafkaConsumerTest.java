package com.yyj.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaConsumerTest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        KafkaConsumer consumer = new KafkaConsumer<>(props);
        while (true) {
            consumer.subscribe(Collections.singleton("yyj1"));
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println("消费到数据：" + record.value());
            }
        }
    }
}
