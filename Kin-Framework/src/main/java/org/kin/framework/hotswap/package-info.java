/**
 * Created by huangjianqin on 2018/2/2.
 */
package org.kin.framework.hotswap;

/*
    目前了解到有3种热加载的方式:
        1.自定义classloader(已实现):
            目前实现两种方式:
                1.1仅能替换成员接口实现类
                1.2基于spring容器

            ps缺陷：需不断创建新的classloader加载新的class并用新的实例替换旧的实例,这里面因为这些classloader都是线性依赖,热更新多次后,并不会回收这些实例,可能导致堆内存增加

        2.Java Agent(已实现):
            通过连接jvm,以agent形式通知jvm加载新的class并替换旧的
            ps:网上说这种方式有性能损失

        3.改造底层JVM行为(如阿里的sandbox jvm,需要很深入了解jvm):
            对业务代码无侵入式
 */