package com.hy.logfaildetect;

import com.hy.logfaildetect.beans.LoginEvent;
import com.hy.logfaildetect.beans.LoginFailWarning;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailWithCep {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = "127.0.0.1";
        int port = 7777;
        DataStream<String> dataStream = env.socketTextStream(host, port);

        /*URL resource = LoginFail.class.getResource("/LoginLog.csv");
        DataStream<String> dataStream = env.readTextFile(resource.getPath());*/
        SingleOutputStreamOperator<LoginEvent> loginEventStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LoginEvent element) {
                return element.getTimeStamp() * 1000;
            }
        });

        // 1. 定义匹配模式
        // firstFail->secondFail within 2s
        Pattern<LoginEvent, LoginEvent> pattern0 = Pattern.<LoginEvent>begin("failFirst").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("failSecond").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).next("thirdFail").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).within(Time.seconds(3));

        Pattern<LoginEvent, LoginEvent> pattern1 = Pattern.<LoginEvent>begin("failEvents").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getLoginState());
            }
        }).times(3).consecutive().within(Time.seconds(3));

        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(LoginEvent::getUserId), pattern1);
        SingleOutputStreamOperator<LoginFailWarning> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, LoginFailWarning>() {
            @Override
            public LoginFailWarning select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent firstFailEvent = map.get("failEvents").get(0);
                LoginEvent lastFailEvent = map.get("failEvents").get(map.get("failEvents").size() - 1);
                return new LoginFailWarning(firstFailEvent.getUserId(), firstFailEvent.getTimeStamp(), lastFailEvent.getTimeStamp(), "login fail " + map.get("failEvents").size() + " times");
            }
        });

    }
}
