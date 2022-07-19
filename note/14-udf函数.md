# UDF函数

## 1. 注册udf函数

```java
// 注册函数
tableEnv.createTemporarySystemFunction("MyFunction", MyFunction.class);
```

## 2. 定义udf函数

### (1). 一对一转化

```java
public static class HashFunction extends ScalarFunction {
    // 接受任意类型输入，返回 INT 型输出
    //DataTypeHint(inputGroup = InputGroup.ANY)对输入参数的类型做了标注，表示 eval 的
	//参数可以是任意类型。
    public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
        return o.hashCode();
    }
}
// 注册函数
tableEnv.createTemporarySystemFunction("HashFunction", HashFunction.class);
// 在 SQL 里调用注册好的函数
tableEnv.sqlQuery("SELECT HashFunction(myField) FROM MyTable");
```

### (2). 表函数 一对多

因为是一对多，一行数据 => 一张表，所有我们不存在select的情况只能作为一个侧视图使用。

LATERAL TABLE(<TableFunction>)

**通过FunctionHint来进行字段名字的定义**

```java
// 注意这里的类型标注，输出是 Row 类型，Row 中包含两个字段：word 和 length的字段名。
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public static class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : str.split(" ")) {
            // 使用 collect()方法发送一行数据
            collect(Row.of(s, s.length()));
        }
    }
}

// 注册函数
tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

// 在 SQL 里调用注册好的函数
// 1. 交叉联结
tableEnv.sqlQuery(
    "SELECT myField, word, length " +
    "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");

// 2. 带 ON TRUE 条件的左联结
tableEnv.sqlQuery(
    "SELECT myField, word, length " +
    "FROM MyTable " +
    "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE");

// 重命名侧向表中的字段
tableEnv.sqlQuery(
    "SELECT myField, newWord, newLength " +
    "FROM MyTable " +
    "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE");
```

### (3).  聚合函数（Aggregate Functions） 多对1

```java
// 累加器类型定义
public static class WeightedAvgAccumulator {
    public long sum = 0; // 加权和
    public int count = 0; // 数据个数
}

// 自定义聚合函数，输出为长整型的平均值，累加器类型为 WeightedAvgAccumulator
public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator>{
    @Override
    public WeightedAvgAccumulator createAccumulator() {
        return new WeightedAvgAccumulator(); // 创建累加器
    }
    @Override
    public Long getValue(WeightedAvgAccumulator acc) {
        if (acc.count == 0) {
            return null; // 防止除数为 0
        } else {
            return acc.sum / acc.count; // 计算平均值并返回
        }
    }
    // 累加计算方法，每来一行数据都会调用
    public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }
}
// 注册自定义聚合函数
tableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);
// 调用函数计算加权平均值
Table result = tableEnv.sqlQuery(
    "SELECT student, WeightedAvg(score, weight) FROM ScoreTable GROUP BY student"
);

```

