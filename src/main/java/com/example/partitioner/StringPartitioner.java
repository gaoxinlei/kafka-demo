package com.example.partitioner;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Random;

/**
 * 自定义分区器
 */
public class StringPartitioner extends Partitioner{

    @Override
    public int partition(ProducerRecord<byte[], byte[]> record, Cluster cluster) {
        return record.topic().hashCode()% new Random(3).nextInt();
    }
}
