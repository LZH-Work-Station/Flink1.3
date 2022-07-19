package com.zehan.operator;

import com.zehan.source.Event;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./home", 4000L),
                new Event("Alice", "./home", 3000L),
                new Event("Mary", "./home", 5000L));

        eventDataStreamSource.map(new RichMapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.user + "Rich function";
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("Start map");
            }

            @Override
            public void close() throws Exception {
                System.out.println("End map");
            }
        }).print();

        env.execute();
    }

}
