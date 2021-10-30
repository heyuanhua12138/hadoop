package com.hyh.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomConsumerAutoCommitOffset {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop104:9092");
        properties.put("group.id", "test");
        //开启自动提交offset功能
        properties.put("enable.auto.commit", "true");
        //自动提交offset的时间间隔
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer kafkaConsumer = new KafkaConsumer<>(properties);
        List<String> topticList = new ArrayList<>();
        topticList.add("hello");
        kafkaConsumer.subscribe(topticList);
        while (true) {
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : consumerRecords){
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
            }
        }

    }

}
