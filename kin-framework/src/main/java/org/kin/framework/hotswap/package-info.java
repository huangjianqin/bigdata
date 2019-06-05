/**
 * Created by huangjianqin on 2018/2/2.
 */
package org.kin.framework.hotswap;

/*
    mark: 类热更新思路->加载新的Class,重新构建实例,把所有引用了该Class的地方重新加载Class(不断传播下去)

    目前了解到有3种热加载的方式:
        1.自定义classloader(已实现):
            目前实现两种方式:
                1.1仅能替换已注册实例的成员域(其实现类=热更的类)
                1.2基于spring容器

            ps缺陷：
                1.需不断创建新的classloader加载新的class并用新的实例替换旧的实例,这里面因为这些classloader都是线性依赖,热更新多次后,并不会回收这些实例,可能导致堆内存增加(方法区也会)
                2.所有实例都需要替换, 需要开发者手动注册这些实例, 不然不知道具体有多少个实例需要替换
                3.需要把当前所有线程的classloader(子类线程的ContextClassLoader由父类派生)替换掉, 然后使用new产生的class(热更的)才生效
            很不完美, 需要自己手动去替换class, 并且很多情况下无法获取到具体实例, 导致无法替换

            该实现方式更适合用于实现热隔离(一个版本一个自定义classloader)
        2.Java Agent(已实现):
            通过连接jvm,以agent形式通知jvm加载新的class并替换旧的, 只适用于方法体变化
            ps:据说有性能损失

        3.改造底层JVM行为(如阿里的sandbox jvm,需要很深入了解jvm):
            对业务代码无侵入式
 */