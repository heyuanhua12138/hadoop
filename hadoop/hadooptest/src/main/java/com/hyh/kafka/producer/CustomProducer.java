package com.hyh.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群，broker-list
        properties.put("bootstrap.servers", "hadoop104:9092");
        properties.put("acks", "all");
        //重试次数
        properties.put("retries", 1);
        //批次大小
        properties.put("batch.size", 16384);
        //等待时间
        properties.put("linger.ms", 1);
        //RecordAccumulator缓冲区大小
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord("hello",Integer.toString(i),Integer.toString(i)));
        }
        kafkaProducer.close();
    }
}
