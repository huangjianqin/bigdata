/**
 * Created by huangjianqin on 2018/2/2.
 */
package org.kin.framework.hotswap.deadcode.spring;
/*
    基于spring
    通过spring容器和机制,获得inject信息和bean信息,可以实现比较全面的热更新
    由于对spring底层了解不够深入,目前仅仅支持@Autowire注解注入方式
 */