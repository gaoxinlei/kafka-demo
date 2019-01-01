package com.example.producer;

import org.junit.Test;

import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 测试string producer
 */
public class StringProducerTest {

    @Test
    public  void testSendString(){
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop130:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 增加服务端请求延时
        props.put("linger.ms", 1);
// 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        StringProducer producer = new StringProducer(props);
        for(int i=0;i<10;i++){
            producer.produce("gao","hello:"+i);
        }
        producer.close();
    }

    /**
     * 测试发送带分区。
     */
    @Test
    public void testSendWithPartitioner(){
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop130:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 增加服务端请求延时
        props.put("linger.ms", 1);
// 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 自定义分区
        props.put("partitioner.class", "com.example.partitioner.StringPartitioner");

        StringProducer producer = new StringProducer(props);
        //topic linux
        tenTimes("linux","linus",producer::produce);
        producer.close();
    }

    private void produceMsg(String topic, String value,KeyValueConsumer consumer) {
        consumer.consume(topic,value);
    }

    private void tenTimes(String topic,String value,KeyValueConsumer consumer){
        for(int i=0;i<10;i++){
            produceMsg(topic,value,consumer);
        }
    }

    @FunctionalInterface
    private interface KeyValueConsumer{
        void consume(String topic,String value);
    }



}
