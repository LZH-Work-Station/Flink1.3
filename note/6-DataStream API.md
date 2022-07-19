# DataStream API

[TOC]

## 1. 执行环境

Flink实现了流批统一的api，不用再区分批处理和流处理。批处理就相当于有界的流处理。

### （1）流处理环境获取

代码自身会进行判断 获得当前执行的运行环境，如果是本地环境就会调用 createLocalEnvironment 创建本地的运行环境。如果是提交到远程的集群环境就会调用createRemoteEnvironment

 ```java
 // 自适应获取上下文执行环境, 获得流处理环境
 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 ```

### （2）批处理环境获取

```java
// 获得批处理环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
```

### （3）批处理和流处理的选择

如果我们只需要一个最终结果，就不需要流处理了，获得完所有数据然后执行批处理就够了。

### （4） 最后的一定需要执行

```java
env.execute();
```

## 2. 获得数据源

### （1）读取文件中的数据

```java
   // 创建执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

// 从文件中中读取数据
DataStreamSource<String> stream1 = env.readTextFile("input/click.txt");

stream1.print();

env.execute();
```

### （2）Kafka中读取数据

```xml
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-connector-kafka_${scala.binary.version}</artifactId>
 <version>${flink.version}</version>
</dependency>
```

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "hadoop102:9092");
properties.setProperty("group.id", "consumer-group");
properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
properties.setProperty("auto.offset.reset", "latest");

// addSource里面 是 "topic"， "反序列化的框架"， "上面定义的schema"
DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("clicks",
	new SimpleStringSchema(), properties));

stream.print("Kafka");
env.execute();
```

## 3. Sink写入数据

### (1) 写入Kafka

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

// addSource里面 是 "topic"， "反序列化的框架"， "上面定义的schema"
DataStreamSource<Event> eventDataStreamSource = env.fromElements(
    new Event("Mary", "./home", 1000L),
    new Event("Bob", "./cart", 2000L),
    new Event("Mary", "./home", 4000L),
    new Event("Alice", "./home", 3000L),
    new Event("Mary", "./home", 5000L));

SingleOutputStreamOperator<String> users = eventDataStreamSource.map(event -> event.user);

users.addSink(new FlinkKafkaProducer<String>("172.21.217.12:9092", "test", new SimpleStringSchema()));
env.execute();
```



### (2) 自定义sink

继承RichSinkFunction 调用invoke方法，每个数据都会执行invoke方法，然后在invoke方法里面写入到自定义的数据库，例如Hbase。

```java
public class SinkCustomtoHBase {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 		env.setParallelism(1);
 		env.fromElements("hello", "world").addSink(
 			new RichSinkFunction<String>() {
                // 管理 Hbase 的配置信息,                                      
                @Override
         public void open(Configuration parameters) throws Exception {
          	super.open(parameters);
         	configuration = HBaseConfiguration.create();
			configuration.set("hbase.zookeeper.quorum", "hadoop102:2181");
 			connection = ConnectionFactory.createConnection(configuration);
 		}
                
 		@Override
		public void invoke(String value, Context context) throws Exception {
 			Table table = connection.getTable(TableName.valueOf("test")); // 表名为 test
			Put put = new Put("rowkey".getBytes(StandardCharsets.UTF_8)); // 指定 rowkey
			put.addColumn("info".getBytes(StandardCharsets.UTF_8), 					 					value.getBytes(StandardCharsets.UTF_8) // 指定列名入的数据
 				, "1".getBytes(StandardCharsets.UTF_8)); // 写入的数据
 			table.put(put); // 执行 put 操作
 			table.close(); // 将表关闭
 		}
                
		@Override
		public void close() throws Exception {
 			super.close();
			connection.close(); // 关闭连接
 		}
 	});
        env.execute();
 	}
}

```



## Flink支持的数据类型

Flink 支持的数据类型 简单来说，对于常见的 Java 和 Scala 数据类型，Flink 都是支持的。Flink 在内部，Flink 对支持不同的类型进行了划分，这些类型可以在 Types 工具类中找到： 

（1）基本类型 所有 Java 基本类型及其包装类，再加上 Void、String、Date、BigDecimal 和 BigInteger。 

（2）数组类型 包括基本类型数组（PRIMITIVE_ARRAY）和对象数组(OBJECT_ARRAY) 

（3）复合数据类型	

- Java 元组类型（TUPLE）：这是 Flink 内置的元组类型，是 Java API 的一部分。最多 25 个字段，也就是从 Tuple0~Tuple25，不支持空字段 
-  Scala 样例类及 Scala 元组：不支持空字段 
-  行类型（ROW）：可以认为是具有任意个字段的元组,并支持空字段 
-  POJO：Flink 自定义的类似于 Java bean 模式的类

### Tuple类型的内部类型的丢失

我们在将 String 类型的每个词转换成（word， count）二元组后，就明确地用 returns 指定了返回的类型。因为对于 map 里传入的 Lambda 表 达式，系统只能推断出返回的是 Tuple2 类型，而无法得到 Tuple2。只有显式地 告诉系统当前的返回类型，才能正确地解析出完整数据。

```java
.map(word -> Tuple2.of(word, 1L))
.returns(Types.TUPLE(Types.STRING, Types.LONG));
```

除此之外还专门提供了一个TypeHints类:

```java
returns(new TypeHint<Tuple2<Types.STRING, Types.LONG>>(){})
```

