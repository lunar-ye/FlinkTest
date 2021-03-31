package com.hy.flink.window;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class Test1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Integer> result1 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10), Time.seconds(2))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };
        SingleOutputStreamOperator<SensorReading> result2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                //.trigger()
                //.evictor()
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");
        result1.print("result1");
        result2.print("result2");
        env.execute();
    }
}
