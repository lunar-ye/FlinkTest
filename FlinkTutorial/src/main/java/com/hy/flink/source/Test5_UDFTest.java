package com.hy.flink.source;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class Test5_UDFTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> dataStream = env.addSource(new MyUDF());
        dataStream.print();
        env.execute();
    }
    private static class MyUDF implements SourceFunction<SensorReading> {

        private boolean running = true;
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();
            HashMap<String, Double> sensorMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorMap.put("sensor" + i, 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sensorID : sensorMap.keySet()) {
                    Double newTemp = sensorMap.get(sensorID) + random.nextGaussian();
                    sensorMap.put(sensorID, newTemp);
                    ctx.collect(new SensorReading(sensorID, System.currentTimeMillis(), newTemp));
                }
            }


        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
