package com.hy.flink.process;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Test2_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("lowTemp"){};
        SingleOutputStreamOperator<SensorReading> highTemp = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {

            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(outputTag, value);
                }
            }
        });
        dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if(value.getTemperature() > 30) {
                    out.collect(value);
                } else {
                    ctx.output(outputTag,value);
                }
            }
        });
        highTemp.print("high");
        DataStream<SensorReading> lowTemp = highTemp.getSideOutput(outputTag);
        lowTemp.print();
        env.execute();
    }
}
