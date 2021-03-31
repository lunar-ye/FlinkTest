package com.hy.flink.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class Test4_OffsetTestAsync {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.247.132:9092");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // consumer手动提交offset
        properties.put("enable.auto.commit", "false");
        properties.put("group.id", "test");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",record.offset(),record.key(),record.value());
            }
            // 同步提交，当前线程会阻塞直到 offset 提交成功
            consumer.commitAsync(new OffsetCommitCallback(){
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if(e != null){
                        System.err.println("commit failed for" + map);
                    }
                }
            });
        }
    }
}
