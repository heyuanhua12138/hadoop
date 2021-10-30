package com.hyh.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomConsumerManualCommitSync {
    public static void main(String[] args) {
        //commitSync（同步提交），commitSync阻塞当前线程，一直到提交成功，并且会自动失败重试（由不可控因素导致，也会出现提交失败）
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop104:9092");
        properties.put("group.id", "test");
        //关闭自动提交offset功能
        properties.put("enable.auto.commit", "false");
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
            //同步提交，当前线程会阻塞直到offset提交成功
            kafkaConsumer.commitSync();
        }

    }

}
