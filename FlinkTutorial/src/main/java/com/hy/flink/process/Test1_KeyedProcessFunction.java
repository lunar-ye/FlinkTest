package com.hy.flink.process;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test1_KeyedProcessFunction {
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
        dataStream.keyBy("id")
                .process(new myProcess(10))
                .print();
        env.execute();
    }
    // 检测一段时间内的温度连续上升，输出报警
    private static class myProcess extends KeyedProcessFunction<Tuple, SensorReading, String> {

        private Integer interval;
        public myProcess(Integer interval) {
            this.interval = interval;
        }

        private ValueState<Long> tsTimeState;
        private ValueState<Double> lastTempState;

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "持续" + interval + "s上升");
            tsTimeState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            tsTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temperature", Double.class, Double.MIN_VALUE));
        }

        @Override
        public void close() throws Exception {
            super.close();
            tsTimeState.clear();
            lastTempState.clear();
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long timerTs = tsTimeState.value();
            if(value.getTemperature() > lastTemp && timerTs == null) {
                Long ts = ctx.timerService().currentProcessingTime()  + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsTimeState.update(ts);
            } else if(value.getTemperature() <= lastTemp && timerTs != null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                tsTimeState.clear();
            }
            lastTempState.update(value.getTemperature());
            /*out.collect(value.getId().length());
            ctx.getCurrentKey();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000L);
            tsTimeState.update(ctx.timerService().currentProcessingTime() + 1000L);*/

        }

    }
}
