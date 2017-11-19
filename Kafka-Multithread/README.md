#Kafka多线程处理实例封装
提供分布式部署,自动化配置及监控
##关键模块
* 消息抓取(OCOT除外)
ConsumerFetcher类,负责单线程从broker抓取record,然后根据topic分区dispatch给MessageHandler.该类还维护了Offset提交队列,每次poll循环时都是把队列中的Offset commit.

* 三种多线程消费及消息处理模式
1. one partition one thread(OPOT)
每个线程维护以一个topic分区,内含dispatch队列,消息按dispatch顺序(接受消费消息顺序)处理.
2. one partition more thread(OPMT, OPMT2)
利用线程池处理消息,这样子无法保证消息处理完成及commit顺序,所以增加了一个收集处理完成的Offset收集类PendingWindow.该类的灵感源自于TCP的滑动窗口(可能实现得有差异,也可能完成不是同一玩意).
每个topic分区对应于一个PendingWindow,消息处理完后Offset提交到PendingWindow处理,PendingWindow维护一个有序队列存储完成的Offset(ConcurrentSkipListSet类型),每次提交Offse到PendingWindow时,如果满足条件(队列大小满足窗口大小或者MessageHandler关闭,也就是窗口大小或者最大的连续的Offset),该线程会持有这个PendingWindow(无锁,因为我们只获取队列的当前视图,不影响后面加入queue的操作),进而判断是否需要commit Offset,其他线程仅仅将完成的Offset进队,然后继续进行消息处理
    PendingWindow提交Offset判断:
        视图=数组
        1.只要获取下标为窗口大小-1的Offset与第一个Offset相减=窗口大小,就是满足,commit Offset
        2.消费者关闭或重新分配分区时,从0开始遍历数组,获取最大的连续的Offset,然后commit
    每次一条消息处理线程持有PendingWindow,执行相应的判断及Commit操作,其余线程继续完成自己的消息处理操作.这样子的设计可以减少锁操作及PendingWindow压力,进而提高效率.
OPMT的线程池相当于任务池,把所有接收到消息,封装task,扔进线程池处理,此处可能存在许多线程切换的开销,所以开发出OPMT2.
OPMT2相当于OPOT的多线程版本,线程池不再是任务池,而是OPOT的消息处理线程,但这里不是1个线程,而是多个,dispatch时,按hashcode % pool.size进行分配,性能相比OPMT有较大提高.

PS:以上两种模式都内置实现了ConsumerRebalanceListener,目的在于重新分配分区时,能够释放部分无用的系统资源,无法保证处理消息exactly once语义.

3. one consumer one thread(OCOT)
每个线程一个消费者,这样子本质与其余模式差不多,但是会对系统有更大消耗.
开发这种模式主要是满足原子存储Offset的场景并保证exactly once语义,这是其余模式做不到,其余模式都会存在重复消费的可能.


开发出配置中心,自动化管理kafka消息处理程序,根据流量动态部署或减少kafka消息处理程序,如动态订阅topic,动态调节线程池资源等等.
模板设计将支持多种格式(底层内部抽象成一种),实现时先实现json模板,模板信息存储在redis.
设计dubbox服务.

* 接下来的工作: