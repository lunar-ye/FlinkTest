package com.hy.hotitemsanalysis;

import com.hy.hotitemsanalysis.beans.ItemViewCount;
import com.hy.hotitemsanalysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Properties;

public class HotItems {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputStream = env.readTextFile("D:\\tencent\\workspace\\FlinkTest\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        env.addSource(new FlinkKafkaConsumer011<String>("hotitems", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000;
            }
        });
        SingleOutputStreamOperator<ItemViewCount> windowAggStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());
        SingleOutputStreamOperator<String> resultStream = windowAggStream.keyBy("windowEnd")
                .process(new TopNHotItems(5));
        resultStream.print();
        env.execute();
    }

    private static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    private static class WindowItemCountResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, java.lang.Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    private static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private int topSize;
        public TopNHotItems(int i) {
            this.topSize = i;
        }

        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
            itemViewCountListState.add(value);
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.getCount() - o1.getCount());
                }
            });
            StringBuilder sb = new StringBuilder();
            sb.append("######################");
            sb.append("窗口结束时间").append(new TimeStamp(timestamp - 1)).append("\n");
            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++ ){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            sb.append("===============================\n\n");

            Thread.sleep(1000);
            out.collect(sb.toString());
        }
    }
}