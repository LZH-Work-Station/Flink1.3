# WaterMark水位线和窗口

[TOC]



## 1. 水位线

一个水位线 Watermark(t)，表示在当前流中事件时间已经达到了时间戳 t, 这代表 t 之 前的所有数据都到齐了，之后流中不会出现时间戳 t’ ≤ t 的数据。水位线是 Flink 流处理中保证结果正确性的核心机制，它往往会跟窗口一起配合，完成对 乱序数据的正确处理。关于这部分内容，我们会稍后进一步展开讲解。

同时我们可以设置延迟，来容忍数据的延迟。

### 水位线的传递和窗口的应用（重要）

上游任务会把自己的水位线告诉给下游的所有任务，比如水位线是9点，那么代表9点之前的数据都到了。但是因为我们的任务是分布式的，所以会导致，上游的多个任务的水位线不一致。比如另一个任务他的水位线到了8点50，所以下游任务会根据**木桶原理**，选择最小的水位线来当作自己的水位线，以保证数据的完整性，所以下游的水位线变成为**8点50**而不是9点。

**直到第二个上游任务的水位线变成9点了**，然后传递给下游，下游才会认为现在全局数据9点之前的数据都到了。

#### 在窗口的应用

通过水位线可以帮助我们完成窗口的应用，比如对于乱序流，我们的容忍是2s，那么当我们想对一个窗口进行计算的时候，比如一个0-10s的窗口，他会等到12s的数据到的时候判断0-10s的数据已经全部都到了，然后再开始计算，用这种办法来避免乱序数据导致的可能数据不完全的情况。

### 乱序流的水位线生成

通过assignTimestampsAndWatermarks设置水位线，forBoundedOutOfOrderness代表是乱序流，可以在后面设置延迟，并且通过withTimestampAssigner指定时间戳

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
SingleOutputStreamOperator<Event> stream = env.fromElements(
        new Event("Mary", "./home", 1000L),
        new Event("Bob", "./cart", 2000L),
        new Event("Tom", "./cart", 3000L),
        new Event("Ken", "./cart", 3500L),
        new Event("Bob", "./cart", 4000L),
        new Event("Lucy", "./cart", 4200L),
        new Event("Bob", "./cart", 5000L),
        new Event("Bob", "./cart", 6000L)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
```

## 2. 窗口的创建

在上面的stream被分配完watermark之后就可以进行窗口的创建，一定要**keyby**，不然无法有并行度

### 滚动窗口

滚动窗口，参数是窗口的大小

```java
stream.keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.hours(1)));
```

### 滑动窗口

滑动窗口，第一个参数是窗口的长度，第二个参数是每个窗口的间距

```java
stream.keyBy(event -> event.user)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(20)));
```

### 会话窗口

间隔指定时间如果没有新的消息就关闭窗口

```java
stream.keyBy(event -> event.user)
                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
```

### 全窗口函数

```java
stream.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
```

## 3. 增量窗口函数（来一条就计算一下）

### 归约函数（Input和Output数据类型一致）

```java
stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return new Tuple2<>(value.user, 1L);
            }
        })
        .keyBy(tuple -> tuple.f0)
    	// 设置窗口
        .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
    	// reduceFunction进行聚合
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        }).print();
```

### 聚合函数（Input和Output数据类型不同）

```java
// In是input数据类型 Acc是聚合的中间值类型 Out是output数据类型
public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable {
	// accumulater的初始值
    ACC createAccumulator();
	// 来一条数据就进行add方法
    ACC add(IN value, ACC accumulator);
	// 获得output
    OUT getResult(ACC accumulator);
	// 两个accumulater的聚合
    ACC merge(ACC a, ACC b);
}
```

## 4. 全窗口函数（窗口watermark到达之后再统一进行计算）

### ProcessWindowFunction

Iterable中包含窗口里面含有的所有数据

```java
stream.keyBy(event -> event.user)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ClickCount()).print();

// <INPUT, OUTPUT, key(keyBy后的key是什么), TimeWindow(固定)>
private static class ClickCount extends ProcessWindowFunction<Event, Tuple2<String, Long>, String, TimeWindow>{
        @Override 
    	// Iterable中包含所有的窗口内收集的数据
        public void process(String s, Context context, Iterable<Event> elements, Collector<Tuple2<String, Long>> out) {
            long timestamp = 0L;
            for(Event event: elements){
                timestamp += event.timestamp;
            }
            out.collect(new Tuple2<>(s, timestamp));
        }
    }
```

## 5. 窗口函数和聚合函数结合（没啥卵用？）

我们之前在调用 WindowedStream 的.reduce()和.aggregate()方法时，只是简单地直接传入 了一个 ReduceFunction 或 AggregateFunction 进行增量聚合。除此之外，其实还可以传入第二 个参数：一个全窗口函数，**可以是 WindowFunction 或者 ProcessWindowFunction。**

这样调用的处理机制是：**基于第一个参数（增量聚合函数）来处理窗口数据，每来一个数 据就做一次聚合**；等到窗口需要触发计算时，则调用第二个参数（全窗口函数）的处理逻辑输 出结果。需要注意的是，这里的全窗口函数就不再缓存所有数据了，而是直接将**增量聚合函数 的结果**拿来当作了 Iterable 类型的输入**(通常只有一个结果，就是前面聚合的结果，感觉没什么卵用，主要是为了聚合之后能获得一些窗口信息罢了)**。一般情况下，这时的可迭代集合中就只有一个元素了。

```java
// ReduceFunction 与 WindowFunction 结合
public <R> SingleOutputStreamOperator<R> reduce(
 ReduceFunction<T> reduceFunction, WindowFunction<T, R, K, W> function) 
// ReduceFunction 与 ProcessWindowFunction 结合
public <R> SingleOutputStreamOperator<R> reduce(
 ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> 
function)
// AggregateFunction 与 WindowFunction 结合
public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
163
 AggregateFunction<T, ACC, V> aggFunction, WindowFunction<V, R, K, W> 
windowFunction)
// AggregateFunction 与 ProcessWindowFunction 结合
public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
 AggregateFunction<T, ACC, V> aggFunction,
 ProcessWindowFunction<V, R, K, W> windowFunction)

```

## 6.侧输出流处理迟到数据

```java
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
        DataStream<Tuple2<String, Long>> sideOutput = result.getSideOutput(late)

```

