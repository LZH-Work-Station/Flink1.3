# Flink DataStream算子

[TOC]

## Transform 转化算子

### map

```java
DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));

// 创建MapFunction类
stream.map(new MapFunction<Event, String>() {
    @Override
    public String map(Event value) throws Exception {
        return value.user;
    }
});

// 传入lambda表达式
stream.map((MapFunction<Event, String>) value -> value.user);
```

### filter

```java
// 创建FilterFunction类
SingleOutputStreamOperator<Event> filter = stream.filter(new FilterFunction<Event>() {
    @Override
    public boolean filter(Event value) throws Exception {
        if (value.user.equals("Mary")) {
            return true;
        }
        return false;
    }
});
// 传入lambda表达式
SingleOutputStreamOperator<Event> filter = stream.filter(value -> {
    if (value.user.equals("Mary")) {
        return true;
    }
    return false;
});
filter.print("Filter");

```

### flatMap

```java
// 先把数据进行打散然后进行map操作
SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap(
    // 使用collector对数据进行收集
    (String line, Collector<Tuple2<String, Long>> collector) -> {
        String[] words = line.split(" ");
        for(String word: words){
            collector.collect(new Tuple2<>(word, 1L));
        }
    }
).returns(Types.TUPLE(Types.STRING, Types.LONG));
```

## Partition分区

### keyBy

```java
// 在Stream里面没有group by, 只有key by 
// 本质是定义一个KeySelector, 实现里面的方法 return的tuple的f0
KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

// 使用 Lambda 表达式
 KeyedStream<Event, String> keyedStream = stream.keyBy(e -> e.user);
// 使用匿名类实现 KeySelector
KeyedStream<Event, String> keyedStream1 = stream.keyBy(new KeySelector<Event, String>(){     @Override
     public String getKey(Event e) throws Exception {
     return e.user;
     }
});
```

### shuffle

```java
// 随机分配
eventDataStreamSource.shuffle().print();
```

### rebalance

```java
// 轮询,根据设定的并行度采用轮询的方式分区
eventDataStreamSource.rebalance().print();
```



## Aggregation聚合算子

### max, maxBy

min()：在输入流上，对指定的字段求最小值。

minBy()：与 min()类似，在输入流上针对指定字段求最小值。不同的是，min()只计 算指定字段的最小值，其他字段会保留最初第一个数据的值；而 minBy()则会返回包 含字段最小值的整条数据。

```java
DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
     Tuple2.of("a", 1),
     Tuple2.of("a", 3),
     Tuple2.of("b", 3),
     Tuple2.of("b", 4)
 );
// minBy是，对于couple 1和"f1"都可以
stream.keyBy(r -> r.f0).min(1).print();
stream.keyBy(r -> r.f0).minBy("f1").print();
```

### sum

```java
// 分组
UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);
//组内聚合
AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);
```

### reduce

```JAVA
T reduce(T value1, T value2)
```

```java
// 案例 找到访问次数最多的user
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
DataStreamSource<Event> eventDataStreamSource = env.fromElements(
    new Event("Mary", "./home", 1000L),
    new Event("Bob", "./cart", 2000L),
    new Event("Mary", "./home", 4000L),
    new Event("Alice", "./home", 3000L),
    new Event("Mary", "./home", 5000L));
// 转化成2元组
SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = eventDataStreamSource
    .map(event -> Tuple2.of(event.user, 1L))
    .returns(Types.TUPLE(Types.STRING, Types.LONG))
    .keyBy(tuple -> tuple.f0)
    .reduce(new ReduceFunction<Tuple2<String, Long>>() {
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
            return Tuple2.of(value1.f0, value1.f1 +value2.f1);
        }});
SingleOutputStreamOperator<Tuple2<String, Long>> max = clicksByUser
    .keyBy(data -> "key")
    .reduce(new ReduceFunction<Tuple2<String, Long>>(){
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

```

