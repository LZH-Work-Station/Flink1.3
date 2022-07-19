# Table API & SQL



## 1. 依赖

[TOC]



### 本地环境

我们想要在代码中使用 Table API，必须引入相关的依赖。用于桥接Table API和底层的DataStream

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-table-api-java-bridge_${scala.binary.version}</artifactId>
	<version>${flink.version}</version>
</dependency>
```

### 线上环境

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner-blink_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```

## 2. 样例

```java
public class TableExample {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
            .fromElements(
            new Event("Alice", "./home", 1000L),
            new Event("Bob", "./cart", 1000L),
            new Event("Alice", "./prod?id=1", 5 * 1000L),
            new Event("Cary", "./home", 60 * 1000L),
            new Event("Bob", "./prod?id=3", 90 * 1000L),
            new Event("Alice", "./prod?id=7", 105 * 1000L)
        );
        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表
        Table eventTable = tableEnv.fromDataStream(eventStream);
        // 用执行 SQL 的方式提取数据
        Table visitTable = tableEnv.sqlQuery("select url, user from " + eventTable);
        // 将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable).print();
        // 执行程序
        env.execute();
    }
}
```

## 3. 创建表环境

### (1). 创建基础环境

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
```

### (2). 注册 Catalog 和表

#### 虚拟表

```java
Table newTable = tableEnv.sqlQuery("SELECT ... FROM MyTable... ");
```

#### 注册表

```java
// 创建虚拟表
tableEnv.createTemporaryView("NewTable", newTable);
// 选择指定字段
tableEnv.createTemporaryView("EventTable", eventStream, $("user"), $("url"), 
$("timestamp").as("ts"));
```

### (3). 执行 SQL 查询

```java
Table urlCountTable = tableEnv.sqlQuery(
    "SELECT user, COUNT(url) " +
    "FROM EventTable " +
    "GROUP BY user "
);
```

### (4). 注册用户自定义函数（UDF）

### (5). DataStream 和表之间的转换

#### 表 => 数据流

##### **将表转换成数据流**

```java
Table aliceVisitTable = tableEnv.sqlQuery(
 "SELECT user, url " +
 "FROM EventTable " +
 "WHERE user = 'Alice' "
 );
// 将表转换成数据流
tableEnv.toDataStream(aliceVisitTable).print();
```

##### 将表转换成日志流（用于随着数据的到来，数据会更新的情况）

如果把流看作一张表，**那么流中每个数据的到来，都应该看作是对表的一次插入（Insert） 操作，会在表的末尾添加一行数据**。因为流是连续不断的，而且之前的输出结果无法改变、只 能在后面追加；所以我们其实是通过一个只有插入操作（insert-only）的更新日志（changelog） 流，来构建一个表。

```java
Table urlCountTable = tableEnv.sqlQuery(
 "SELECT user, COUNT(url) " +
 "FROM EventTable " +
 "GROUP BY user "
 );
// 将表转换成更新日志流
tableEnv.toChangelogStream(urlCountTable).print();
```

#### 将数据流转换成表

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 获取表环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 读取数据源
SingleOutputStreamOperator<Event> eventStream = env.addSource(...)
// 将数据流转换成表
Table eventTable = tableEnv.fromDataStream(eventStream);
```

## 4. 设置水位线

```java
// 读取数据源，并分配时间戳、生成水位线
SingleOutputStreamOperator<Event> eventStream = env
    .fromElements(
    new Event("Alice", "./home", 1000L),
    new Event("Bob", "./cart", 1000L),
    new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
    326
    new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
    new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
    new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
    new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L) 
)
    .assignTimestampsAndWatermarks(
    WatermarkStrategy.<Event>forMonotonousTimestamps()
    .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
        @Override
        public long extractTimestamp(Event element, long recordTimestamp) {
            return element.timestamp;
        }
    })
);

// 创建表环境
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
// 将数据流转换成表，并指定时间属性
Table eventTable = tableEnv.fromDataStream(
    eventStream,
    $("user"),
    $("url"),
    $("timestamp").rowtime().as("ts") 
    // 将 timestamp 指定为事件时间，并命名为 ts
);

```

## 5. 窗口

### （1）滚动窗口

```java
TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR)
```

```java
Table result = tableEnv.sqlQuery(
    "SELECT " +
    "user, " +
    "window_end AS endT, " +
    "COUNT(url) AS cnt " +
    "FROM TABLE( " +
    "	TUMBLE( TABLE EventTable, " +
    "	DESCRIPTOR(ts), " +
    "	INTERVAL '1' HOUR)) " +
    "GROUP BY user, window_start, window_end "
); 
```

### （2）滑动窗口

```java
HOP(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '5' MINUTES, INTERVAL '1' HOURS));
```

```java
Table result = tableEnv.sqlQuery(
    "SELECT " +
    "user, " +
    "window_end AS endT, " +
    "COUNT(url) AS cnt " +
    "FROM TABLE( " +
    "	HOP(TABLE EventTable, DESCRIPTOR(ts), " + 
    "	INTERVAL '5' MINUTES, INTERVAL '1' HOURS))" +
    "GROUP BY user, window_start, window_end "
); 
```

### （3）累积窗口

第一个时间是每次增加的长度，第二个是最大长度。例如我们想统计一天中用户的数量，每一个小时就通报一次。即窗口的长度就是每次增加1小时，直到增加到最大长度1天。

```java
CUMULATE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOURS, INTERVAL '1' DAYS))
```

```java
Table result = tableEnv.sqlQuery(
    "SELECT " +
    "user, " +
    "window_end AS endT, " +
    "COUNT(url) AS cnt " +
    "FROM TABLE( " +
    "	CUMULATE(TABLE EventTable, DESCRIPTOR(ts), " + 
    "	INTERVAL '1' HOURS, INTERVAL '1' DAYS))" +
    "GROUP BY user, window_start, window_end "
); 
```

## 6. 开窗聚合(Over)

选择指定范围进行开窗，每来一条数据都会进行计算，而不像之前的窗口聚合是等待watermark到达指定的window end选择这个窗口内的所有数据的聚合值。

```java
SELECT
 <聚合函数> OVER (
 [PARTITION BY <字段 1>[, <字段 2>, ...]]
 ORDER BY <时间属性字段>
 <开窗范围>),
 ...
FROM ...
```

### (1) 从某一时刻到现在

```java
RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
```

```sql
SELECT user, ts,
 COUNT(url) OVER (
     PARTITION BY user
     ORDER BY ts
     RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
 ) AS cnt
FROM EventTable
```

### (2) 从某一行到当前行

```java
ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
```

```sql
SELECT user, ts,
 COUNT(url) OVER w AS cnt,
 MAX(CHAR_LENGTH(url)) OVER w AS max_url
FROM EventTable
WINDOW w AS (
    PARTITION BY user
    ORDER BY ts
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
```

## 7.  表的联结

### （1）Append Only表

```sql
SELECT *
FROM Order o, Shipment s
WHERE o.id = s.order_id
AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time
```

### （2）change log表

```sql
就叫作“时间联结”（Temporal Join）
```

