package com.hy.flink.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Test1_APITest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.247.132:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("ack","all");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        System.out.println("success:" + recordMetadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });//阻塞.get();
        }
        producer.close();

    }
}
