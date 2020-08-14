package org.kin.kafka.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * Created by hjq on 2017/6/19.
 * 消息处理具体逻辑
 */
public interface MessageHandler<K, V> {
    /**
     * 初始化
     */
    void setup(Properties config) throws Exception;

    /**
     * 消息处理
     */
    void handle(ConsumerRecord<K, V> record) throws Exception;

    /**
     * 释放占用资源
     */
    void cleanup() throws Exception;
}
