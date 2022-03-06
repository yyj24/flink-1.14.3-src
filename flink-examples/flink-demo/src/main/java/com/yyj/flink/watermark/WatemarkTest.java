package com.yyj.flink.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.time.Duration;

/**
 * 参考链接：https://zhuanlan.zhihu.com/p/364013202
 */
public class WatemarkTest {
    public void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        //设置WatermarkStrategy方式1
        dataStream.map(new MapFunction<String, MyEvent>() {

            @Override
            public MyEvent map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new MyEvent(fields[0], Integer.valueOf(fields[1]), Long.valueOf(fields[1]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<MyEvent>forBoundedOutOfOrderness(Duration.ofMillis(10))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp));

        //设置WatermarkStrategy方式2
        dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            private final long maxOutOfOrderness = 1000; // 1 seconds
            private long currentMaxTimestamp;

            @Override
            public Watermark getCurrentWatermark() {
                // 当前最大时间戳减去maxOutOfOrderness，就是watermark
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                //从输入抽取时间时间
                long timestamp = 0l;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });

        //设置WatermarkStrategy方式3
        dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(String element) {
                //从输入抽取时间时间
                return 0;
            }
        });
    }

    static class MyEvent implements Serializable {
        private String name;
        private int age;
        private long timestamp;

        public MyEvent() {
        }

        public MyEvent(String name, int age, long timestamp) {
            this.name = name;
            this.age = age;
            this.timestamp = timestamp;
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

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
