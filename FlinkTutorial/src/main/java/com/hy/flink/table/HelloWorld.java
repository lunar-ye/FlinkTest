package com.hy.flink.table;

import javafx.scene.control.Tab;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class HelloWorld {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        EnvironmentSettings oldSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        // 1.1 基于老版本的流处理
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldSettings);
        // 1.2 基于老版本的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);
        // 1.3 基于blink的流处理
        EnvironmentSettings blinkStreamTableSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamTableSettings);
        // 1.4 基于blink的批处理
        EnvironmentSettings blinkBatchTableSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchTableSettings);

        // 2. 表的创建
        // 2.1 读取文件
        String filePath = "D:\\tencent\\workspace\\FlinkTest\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("timestamp", DataTypes.BIGINT())
                    .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");
        tableEnv.connect(new Kafka().version("0.11")
                .topic("test")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092"))
        .withFormat(new OldCsv())
        .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable1");

        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id, temperature")
                .filter("id='sensor_1'");
        resultTable.printSchema();
        tableEnv.toAppendStream(resultTable, Row.class).print();

        Table aggTable = sensorTable.groupBy("id")
                .select("id, id.count as cnt, temperature.avg as avgTemp");
        Table sqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temperature) as tmp from inputTable " +
                "group by id");
        tableEnv.toRetractStream(sqlTable, Row.class).print("sqlagg");
        env.execute();

    }
}
