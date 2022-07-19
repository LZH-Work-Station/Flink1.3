package com.zehan.operator;

import com.zehan.source.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./home", 4000L),
                new Event("Alice", "./home", 3000L),
                new Event("Mary", "./home", 5000L));
        // 转化成2元组
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = eventDataStreamSource.map(event -> Tuple2.of(event.user, 1L)).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
                        return Tuple2.of(value1.f0, value1.f1 +value2.f1);
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, Long>> max = clicksByUser.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                if (value1.f1 > value2.f1) {
                    return value1;
                } else {
                    return value2;
                }
            }
        });


        max.print("max");
        env.execute();
    }
}
