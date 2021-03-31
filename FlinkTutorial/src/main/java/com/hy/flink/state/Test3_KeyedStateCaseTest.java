package com.hy.flink.state;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test3_KeyedStateCaseTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]), new Double(fields[2]));
        });
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = dataStream.keyBy("id")
                .flatMap(new TempChangeWarning(10.0));
        result.print();
        env.execute();
    }
    private static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        }

        @Override
        public void close() throws Exception {
            super.close();
            lastTempState.clear();
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if(lastTemp != null) {
                Double diff = Math.abs(value.getTemperature() - lastTemp);
                if(diff >= threshold) {
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
                }
                lastTempState.update(value.getTemperature());
            }
        }
    }
}
