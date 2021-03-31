package com.hy.flink.transform;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class Test4_MultipleStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]), new Double(fields[2]));
        });

        // 分流
        SplitStream<SensorReading> splitStream = dataStream.split((value -> {
            if (value.getTemperature() > 30) {
                return Collections.singletonList("high");
            } else {
                return Collections.singletonList("low");
            }
        }));
        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        DataStream<SensorReading> allStream = splitStream.select("high", "low");

        // 合流
        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warningStream.connect(lowStream);
        DataStream<Object> result = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {

            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple3<>(value.getId(), value.getTemperature(), "normal");
            }
        });
        result.print("result");

        // 3. union联合多条流
        DataStream<SensorReading> unionStream = highStream.union(lowStream, allStream);
        unionStream.print("union stream");
        env.execute();


    }
}
