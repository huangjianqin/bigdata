##基于Redis和Zookeeper的分布式锁实现
###1基于Redis的实现
* **支持阻塞锁和超时锁**
* 利用setnx尝试抢占锁,同时通过设置key的value为超时时间来处理突然dead而不能释放锁的进程.

###2基于Zookeeper的实现
* **支持阻塞锁和超时锁**
* **Zookeeper原生APIs实现**
* 利用多个进程抢占同一路径的方法来实现,同时该path是"临时的",也就是如果持有锁的进程突然dead.会释放锁
