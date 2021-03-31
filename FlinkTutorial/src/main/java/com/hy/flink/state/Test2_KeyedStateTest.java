package com.hy.flink.state;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test2_KeyedStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]), new Double(fields[2]));
        });
        SingleOutputStreamOperator<Integer> result = dataStream.keyBy("id")
                .map(new MyKeyedMap());
        result.print();
        env.execute();
    }
    private static class MyKeyedMap extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> keyCountState;
        private ListState<String> listCountState;
        private MapState<String, Double> mapState;
        private ReducingState<SensorReading> reducingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key_count", Integer.class, 0));
            listCountState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list",String.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            for(String str : listCountState.get()) {
                System.out.println(str);
            }
            listCountState.add("hello" + value.getId());
            mapState.put("1", 123.1);
            mapState.get("1");
            mapState.remove("1");
            mapState.clear();
            //reducingState.add(value);
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }
}
