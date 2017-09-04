package org.kin.kafka.multithread.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by hjq on 2017/6/19.
 * 消息处理具体逻辑
 */
public interface MessageHandler<K, V> {
    /**
     * 初始化
     * @throws Exception
     */
    void setup() throws Exception;

    /**
     * 消息处理
     * @param record
     * @throws Exception
     */
    void handle(ConsumerRecord<K, V> record) throws Exception;

    /**
     * 释放占用资源
     * @throws Exception
     */
    void cleanup() throws Exception;
}
