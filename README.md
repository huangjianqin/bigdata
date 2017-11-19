# BigData
对大数据及分布式相关技术的研究和实践
主要使用java和scala语言实现，python实现在[这里](https://github.com/huangjianqin/MLandDM)

##1.基于Redis和Zookeeper的分布式锁实现
###1.1基于Redis的实现
* **支持阻塞锁和超时锁**
* 利用setnx尝试抢占锁,同时通过设置key的value为超时时间来处理突然dead而不能释放锁的进程.

###1.2基于Zookeeper的实现
* **支持阻塞锁和超时锁**
* **Zookeeper原生APIs实现**
* 利用多个进程抢占同一路径的方法来实现,同时该path是"临时的",也就是如果持有锁的进程突然dead.会释放锁

##2.简单的分布式(微)服务开发,管理和监控,并实现一个简易Web App用于测试,暂定场景是电商.
##3.数据挖掘实践.