package com.hy.networkflowanalysis;

import com.hy.networkflowanalysis.beans.ApacheLogEvent;
import com.hy.networkflowanalysis.beans.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.Iterable;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*URL resource = HotPages.class.getResource("/apache.log");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());*/
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);

        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimestamp();
            }
        });
        dataStream.print("data");

        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream.filter(data -> "GET".equals(data.getMethod()))
                .filter(data->{
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(data -> data.getUrl())
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult())
                ;
        windowAggStream.print("agg");
        windowAggStream.getSideOutput(lateTag).print("late");

        SingleOutputStreamOperator<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));
        resultStream.print("result");
        env.execute("");
    }

    private static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String s, TimeWindow window, java.lang.Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(s, window.getEnd(), input.iterator().next()));
        }
    }

    private static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private int topSize;
        public TopNHotPages(int i) {
            this.topSize = i;
        }
        MapState<String, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("page-count-map",String.class, Long.class));
        }

        @Override
        public void close() throws Exception {
            mapState.clear();
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> out) throws Exception {
            mapState.put(value.getUrl(), value.getCount());
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(mapState.entries());
            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if(o1.getValue() > o2.getValue())
                        return -1;
                    else if(o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });
            // 格式化成String输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1000)).append("\n");

            // 遍历列表，取top n输出
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(currentItemViewCount.getKey())
                        .append(" 浏览量 = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
