package com.yyj.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Properties;
import java.util.Random;


public class KafkaProducerTest {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProps.BOOTSTRAP_SERVERS_CONFIG);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerTest");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<>(props);
        Random random = new Random();
        while (true) {
            String msg = JSON.toJSONString(new User("user" + random.nextInt(20), random.nextInt(20)));
            ProducerRecord record = new ProducerRecord<>("yyj1", msg);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("生产数据成功：" + record.toString());
                    }
                }
            });
            Thread.sleep(1000);
        }
    }

    static class User implements Serializable {
        private String name;
        private int age;

        public User() {
        }

        public User(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
