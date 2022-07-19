package com.zehan.operator;

import com.zehan.source.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapOperation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));
        stream.map((MapFunction<Event, String>) value -> value.user);

        SingleOutputStreamOperator<Event> filter = stream.filter(value -> {
            if (value.user.equals("Mary")) {
                return true;
            }
            return false;
        });
        filter.print("Filter");


        //stream.print("Map");
        env.execute();

    }
}
