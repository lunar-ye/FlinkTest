package com.hy.flink.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Test5_InterceptorProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.247.132:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("ack","all");
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.hy.flink.kafka.TimeInterceptor");
        interceptors.add("com.hy.flink.kafka.CounterInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test", "message" + i);
            producer.send(record);
        }
        producer.close();
    }
}
