# kin-kafka
kafka工具类
## Kafka多线程消费封装
### OPMT(one partition more thread)
`KafkaMessageFetcher`, 负责单线程从kafka broker抓取record, 然后根据topic分区dispatch给多个线程处理.
每次poll循环时都会把队列中的待提交的offset commit.

#### `PendingWindow`
由于利用多线程处理消息, 这样子无法保证offset commit顺序, 所以需要收集处理完成的Offset. 
灵感源自于TCP的滑动窗口(可能实现得有差异,也可能完成不是同一玩意). 
每个topic分区对应于一个`PendingWindow`, 消息处理完后Offset提交到`PendingWindow`处理, `PendingWindow`维护一个有序队列存储完成的Offset(`ConcurrentSkipListSet`类型), 
每次提交Offse到`PendingWindow`时, 如果满足条件(队列大小满足窗口大小或者手动触发offset提交), 
该线程会持有这个`PendingWindow`(无锁), 然后完成offset提交操作, 此时其余线程仍然自己继续处理消息, 并提交offset到`PendingWindow`.
offset提交操作会把队列中最大连续的offset提交到`KafkaOffsetManager`, 最后在下一轮poll时, consumer会提交到kafka broker.

PS: 内置实现了ConsumerRebalanceListener, 目的在于重新分配分区时, 能够释放部分无用的系统资源, 无法保证处理消息exactly once语义.

### OCOT(one consumer one thread)
主要适用于atomic存储Offset的场景并保证exactly once语义
