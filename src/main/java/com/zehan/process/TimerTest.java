package com.zehan.process;

import com.zehan.source.ClickSource;
import com.zehan.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;

public class TimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.keyBy(value -> value.user)
            .process(new KeyedProcessFunction<String, Event, String>() {
                @Override
                public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                    // 获得当前watermark，这里拿到的是event中注册的timestamp，但是timeService.currentWaterMark会有个延后，他不足够及时
                    long currTs = ctx.timestamp();
                    out.collect("数据到达，到达时间：" + new Timestamp(currTs) + "，水位线为: " + new Timestamp(ctx.timerService().currentWatermark()));
                    // 注册一个 10 秒后的定时器 来一条注册一个定时器
                    ctx.timerService().registerEventTimeTimer(currTs + 10L);
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    // 定时器时间到达 触发下面操作的回调
                    out.collect("定时器触发，触发时间：" + timestamp);
                }
            }).print();

        env.execute();
    }
}
