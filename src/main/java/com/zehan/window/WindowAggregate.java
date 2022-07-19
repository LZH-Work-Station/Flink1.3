package com.zehan.window;

import com.zehan.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WindowAggregate {
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
        // 创建测输出流
        OutputTag<Tuple2<String, Long>> late = new OutputTag<Tuple2<String, Long>>("late"){};
        SingleOutputStreamOperator<Tuple2<String, Long>> result = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return new Tuple2<>(value.user, 1L);
                    }
                })
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .sideOutputLateData(late)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });
        // 将侧输出流里面的数据取出 拿到datastream
        DataStream<Tuple2<String, Long>> sideOutput = result.getSideOutput(late);

        //
        stream.keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                    // 设置初始值
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                    // 每来一次就会运行一次add方法
                    @Override
                    public Long add(Event value, Long accumulator) {
                        return accumulator + 1;
                    }
                    //
                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a+b;
                    }
                }).print();

        //stream.keyBy(event -> event.user)
        //        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(20)));
        //
        //stream.keyBy(event -> event.user)
        //        .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
        env.execute();
    }
}
