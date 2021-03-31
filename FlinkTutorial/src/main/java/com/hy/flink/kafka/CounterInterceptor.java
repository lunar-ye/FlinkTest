package com.hy.flink.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor {
    private int errorCounter = 0;
    private int successCounter = 0;
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null) successCounter++;
        else errorCounter++;
    }

    @Override
    public void close() {
        System.out.println("success sent " + successCounter);
        System.out.println("Failed sent " + errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
