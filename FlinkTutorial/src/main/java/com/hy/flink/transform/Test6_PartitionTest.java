package com.hy.flink.transform;

import com.hy.flink.beans.SensorReading;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test6_PartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]), new Double(fields[2]));
        });
        dataStream.print("input");

        DataStream<SensorReading> shuffleStrem = dataStream.shuffle();
        // shuffleStrem.print("shuffle");
        DataStream<SensorReading> rebalanceStream = dataStream.rebalance();
        // rebalanceStream.print("rebalance stream");
        DataStream<SensorReading> globalStream = dataStream.global();
        //globalStream.print("global");
        int num = env.getParallelism();
        DataStream<SensorReading> result = dataStream.partitionCustom(new Partitioner<Long>() {
            @Override
            public int partition(Long key, int numPartitions) {
                return (int) (key % num);
            }
        }, "timestamp");
        result.print("result");
        env.execute();

    }
}
