package com.hy.logfaildetect;

import com.hy.logfaildetect.beans.LoginEvent;
import com.hy.logfaildetect.beans.LoginFailWarning;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

public class LoginFail {
    public static void main(String[] args) throws Exception {
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
        loginEventStream.print("event");
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectFunction(2));
        warningStream.print("warning");
        env.execute();


    }

    private static class LoginFailDetectFunction extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        private Integer maxFailTimes;
        ListState<LoginEvent> loginEventListState;
        ValueState<Long> timerTsState;

        public LoginFailDetectFunction(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            loginEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>(" fail-list", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void close() throws Exception {
            super.close();
            loginEventListState.clear();
            timerTsState.clear();
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            System.out.println(value);
            if ("fail".equals(value.getLoginState())) {
                loginEventListState.add(value);
                if (timerTsState.value() == null) {
                    timerTsState.update((value.getTimeStamp() + 2) * 1000);
                    ctx.timerService().registerEventTimeTimer(timerTsState.value());
                }
            } else {
                if (timerTsState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                }
                loginEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            System.out.println("on timer");
            super.onTimer(timestamp, ctx, out);
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(loginEventListState.get().iterator());
            Integer failTimes = loginEvents.size();
            System.out.println(failTimes);
            System.out.println(ctx.getCurrentKey());
            if (failTimes >= maxFailTimes) {
                out.collect(new LoginFailWarning(ctx.getCurrentKey(), loginEvents.get(0).getTimeStamp(),
                        loginEvents.get(failTimes - 1).getTimeStamp(), "login fail " + failTimes));
            }
            loginEventListState.clear();
            timerTsState.clear();

        }
    }
    private static class LoginFailDetectFunction1 extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {
        private Integer maxFailTimes;
        private ListState<LoginEvent> listState;

        public LoginFailDetectFunction1(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-event", LoginEvent.class));
        }

        @Override
        public void close() throws Exception {
            super.close();
            listState.clear();
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
            if ("fail".equals(value.getLoginState())) {
                Iterator<LoginEvent> iterator = listState.get().iterator();
                if (iterator.hasNext()) {
                    LoginEvent preLoginEvent = iterator.next();
                    if (value.getTimeStamp() - preLoginEvent.getTimeStamp() <= 2) {
                        out.collect(new LoginFailWarning(ctx.getCurrentKey(), preLoginEvent.getTimeStamp(),
                                value.getTimeStamp(), "login fail \n"));
                    }
                    listState.clear();
                    listState.add(value);
                }
            } else {
                listState.clear();
            }
        }
    }
}
