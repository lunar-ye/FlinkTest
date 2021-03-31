package com.hy.flink.transform;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test5_RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]), new Double(fields[2]));
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getId().length());
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open");
            }

            @Override
            public void close() throws Exception {
                System.out.println("close");
            }
        });
        result.print();
        env.execute();
    }
}
