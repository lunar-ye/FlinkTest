package com.hy.networkflowanalysis;

import com.hy.networkflowanalysis.beans.PageViewCount;
import com.hy.networkflowanalysis.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;

public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = UniqueVisitor.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());
        uvStream.print();
        env.execute();
    }

    private static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class MyBloomFilter {
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }

        public Long hashCode(String value, Integer sed) {
            Long result = 0L;
            for(int i = 0; i < value.length(); i++) {
                result = result * sed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }
    private static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        private Jedis jedis;
        private MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            myBloomFilter = new MyBloomFilter(1 << 29);
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> out) throws Exception {
            Long windowEnd = context.window().getEnd();
            String bitmapkey = windowEnd.toString();

            String countName = "uv_count";
            String countKey = windowEnd.toString();

            Long userId = elements.iterator().next().getUserId();
            Long offset = myBloomFilter.hashCode(userId.toString(), 25);

            Boolean isExist = jedis.getbit(bitmapkey, offset);
            if(!isExist) {
                jedis.setbit(bitmapkey, offset, true);
                Long uvCount = 0L;
                String uvCountString = jedis.hget(countName, countKey);
                if(uvCountString != null && !"".equals(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countName,countKey,String.valueOf(uvCount + 1));
                out.collect(new PageViewCount("uv", windowEnd, uvCount + 1));
            }

        }
    }
}
