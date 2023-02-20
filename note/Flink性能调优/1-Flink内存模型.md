# Flink内存模型

## 1. 内存模型

- JVM执行开销（默认0.1*总内存）
- 框架内存（TaskManager本身所占内存，**不计入slot可用内存**）
- Task内存（Task执行用户代码的内存）

​			大小 = 总内存 - 其他内存

- 网络内存（主要用来网络数据缓冲区的内存）
- 托管内存（用于RocksDB State Backend）的本地内存和批的排序，哈希表，缓存中间结果，给KV类型的状态使用的**（如果不使用 RocksDB存储状态而直接使用内存的话，可以把托管内存设为0）**
- ![image-20230130005315962](images/image-20230130005315962.png)

​	可以在这个里面看内存占用量，不够就多申请一些内存

## 2. 内存配置策略

```shell
bin/flink run \
-d \
-p 5 \ 
-Dyarn.application.queue=test \ # 指定yarn的运行队列
-Djobmanager.memory.process.size=2048mb \
-Dtaskmanager.memory.process.size=6144mb \
-Dtaskmanager.numberOfTaskSlot=2 \ # 与容器的核数相同，即1core: 1slot
-c com.atguigu.flink.tuning.UvDemo \ 
/opt/module/flink-1.13.1/flink-tuning-1.0-SNAPSHOT.jar
```

### yarn 调度队列

#### FIFO队列

FIFO Scheduler把应用按提交的顺序排成一个队列，这是一个先进先出队列，在进行资源分配的时候，**先给队列中最头上的应用分配资源，待最头上的应用需求满足后再给下一个分配，以此类推。**

FIFO Scheduler是最简单也是最容易理解的调度器，不需要任何配置，但其不适用于共享集群。**大的应用可能会占用所有集群资源，这就导致其它应用被阻塞。**在共享集群中，更适合采用Capacity Scheduler或Fair Scheduler，这两种调度器都允许大任务和小任务在提交的同时获得一定的资源。

#### Capacity调度器

有一个**专门的队列用来运行小任务，每个队列都是一个FIFO调度器，同时只能运行一个任务**，但是为小任务专门设置一个队列会占用一定的集群资源，**这就导致大任务的执行时间会落后于使用FIFO调度器时的时间**。

#### Fair调度器

Fair调度器会为所有运行的job动态的调整系统资源。如下图所示，**当第一个大job提交时，只有这一个job在运行，此时它获得了所有集群资源；当第二个小任务提交后，Fair调度器会分配一半资源给这个小任务，让这两个任务公平的共享集群资源。**

需要注意的是，在下图Fair调度器中，从第二个任务提交到获得资源会有一定的延迟，因为它需要等待第一个任务释放占用的Container。小任务执行完成以后也会释放自己占用的资源，大任务又获得了全部的系统资源。**最终的效果就是Fair调度器既得到了高资源的利用率又能保证小任务的及时执行。**