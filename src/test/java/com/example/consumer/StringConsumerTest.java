package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class StringConsumerTest {

    @Test
    public void testConsume() {

        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "hadoop102:9092");
        // 制定consumer group
        props.put("group.id", "test");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe("gao", "linux", "ericsson");

        while (true) {
            // 读取数据，读取超时时间为100ms
            Map<String, ConsumerRecords<String, String>> recordMap = consumer.poll(100);

            for (ConsumerRecords<String, String> records : recordMap.values()) {
                List<ConsumerRecord<String, String>> recordList = records.records(0);
                recordList.forEach(record -> {
                    try {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

        }
    }

}

