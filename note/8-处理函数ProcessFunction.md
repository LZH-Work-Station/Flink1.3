# 处理函数

## 1. ProcessFunction

Process Function可以更加灵活的对每个element进行操作，我们可以自定义一个processFunction然后用collector去收集转化后的数据

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
env.addSource(new ClickSource())
	.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
 	.withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
         @Override
        public long extractTimestamp(Event event, long l) {
 			return event.timestamp;
 		}
 	}))
 	.process(new ProcessFunction<Event, String>() {
     @Override
     	public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
 			if (value.user.equals("Mary")) {
 				out.collect(value.user);
 			} else if (value.user.equals("Bob")) {
 				out.collect(value.user);
				out.collect(value.user);
 			}
 			System.out.println(ctx.timerService().currentWatermark());
 		}
 	}).print();

```

## 2. KeyedProcessFunction

- 拥有定时功能，能够定时操作
- 通过context中的TimeServer可以拿到当前的时间

```java
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

```

### 水位线和时间戳的比较

水位线总是和上一条数据的时间戳相同，因为只有数据被处理完成才会更新时间戳

```java
数据到达，到达时间：2022-05-29 02:59:58.67，水位线为: 292278994-08-17 15:12:55.192
数据到达，到达时间：2022-05-29 02:59:59.682，水位线为: 2022-05-29 02:59:58.669
定时器触发，触发时间：1653764398680
数据到达，到达时间：2022-05-29 03:00:00.696，水位线为: 2022-05-29 02:59:59.681
定时器触发，触发时间：1653764399692
数据到达，到达时间：2022-05-29 03:00:01.711，水位线为: 2022-05-29 03:00:00.695
定时器触发，触发时间：1653764400706
数据到达，到达时间：2022-05-29 03:00:02.717，水位线为: 2022-05-29 03:00:01.71
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

