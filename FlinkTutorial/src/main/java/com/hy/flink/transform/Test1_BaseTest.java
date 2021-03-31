package com.hy.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

public class Test1_BaseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 1. map
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        // 2. flatmap
        DataStream<Integer> flatmapStream = inputStream.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public void flatMap(String value, Collector<Integer> out) throws Exception {
                String[] fields = value.split(",");
                for(String field : fields) {
                    out.collect(field.length());
                }
            }
        });

        // 3. filter
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        mapStream.print("map stream");
        flatmapStream.print("flatmap stream");
        filterStream.print("filter stream");
        env.execute();
    }
}
