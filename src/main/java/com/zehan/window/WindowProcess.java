package com.zehan.window;

import com.zehan.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WindowProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Tom", "./cart", 3000L),
                new Event("Ken", "./cart", 3500L),
                new Event("Bob", "./cart", 4000L),
                new Event("Lucy", "./cart", 4200L),
                new Event("Bob", "./cart", 5000L),
                new Event("Bob", "./cart", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));

        stream.keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ClickCount()).print();

        env.execute();


    }
    // <INPUT, OUTPUT, key(keyBy后的key是什么), TimeWindow(固定)>
    private static class ClickCount extends ProcessWindowFunction<Event, Tuple2<String, Long>, String, TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Event> elements, Collector<Tuple2<String, Long>> out) {
            long timestamp = 0L;
            for(Event event: elements){
                timestamp += event.timestamp;
            }
            out.collect(new Tuple2<>(s, timestamp));
        }
    }
}
