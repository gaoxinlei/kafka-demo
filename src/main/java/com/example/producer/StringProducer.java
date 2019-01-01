package com.example.producer;

import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

/**
 * topic  是code的生产者,字段类型字符串。
 */
@Data
public class StringProducer {

    private Properties config;
    private KafkaProducer<String,String> producer;
    private static final Logger LOGGER = LoggerFactory.getLogger(StringProducer.class);

    /**
     * 构建producer，使用后需要close
     * @param properties
     */
    public StringProducer(Properties properties){
        this.config = properties;
        this.producer = new KafkaProducer<String, String>(properties);
        LOGGER.info("producer 成功构建,properties:{}",properties);
    }

    /**
     * 生产1消息。
     * 生产后需要关闭producer。
     */
    public void produce(String topic,String value){
        producer.send(new ProducerRecord<>(topic,value),(meta,exception)->{
            Optional.ofNullable(meta).ifPresent(data->{
                LOGGER.info("partition:{},offset:{}",data.partition(),data.offset());
            });
            Optional.ofNullable(exception).ifPresent(ex->{
                LOGGER.error("发生错误:{}",ex);
            });
        });
    }

    /**
     * 关闭producer
     */
    public void close(){
        this.producer.close();
        LOGGER.info("producer 成功关闭。");
    }
}
