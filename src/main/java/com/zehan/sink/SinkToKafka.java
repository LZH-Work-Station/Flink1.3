package com.zehan.sink;

import com.zehan.source.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // addSource里面 是 "topic"， "反序列化的框架"， "上面定义的schema"
        DataStreamSource<Event> eventDataStreamSource = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./home", 4000L),
                new Event("Alice", "./home", 3000L),
                new Event("Mary", "./home", 5000L));

        SingleOutputStreamOperator<String> users = eventDataStreamSource.map(event -> event.user);

        users.addSink(new FlinkKafkaProducer<String>("172.21.217.12:9092", "test", new SimpleStringSchema()));
        env.execute();
    }
}
