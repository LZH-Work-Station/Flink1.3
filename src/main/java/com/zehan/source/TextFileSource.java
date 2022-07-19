package com.zehan.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TextFileSource {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件中中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/click.txt");

        stream1.print();

        env.execute();
    }
}
