# Flink 并行度设置

## 1. 并行度设置的方法

在代码中我们可以对每一个算子进行并行度的设置，但是keyBy返回DataStream的这种是无法设置并行度的：

```java
// 全局设置并行度
env.setParallelism(1);
// 算子可以设置并行度
SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap(
    (String line, Collector<Tuple2<String, Long>> collector) -> {
        String[] words = line.split(" ");
        for(String word: words){
            collector.collect(new Tuple2<>(word, 1L));
        }
    }
).setParallelism(3).returns(Types.TUPLE(Types.STRING, Types.LONG));
```



