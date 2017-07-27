package org.kin.kafka.multithread.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by hjq on 2017/6/19.
 * 消息处理的回调接口
 */
public interface CallBack {
    /**
     * 初始化回调接口
     * @throws Exception
     */
    void setup() throws Exception;
    /**
     * 处理成功时Exception e为null
     * @param record
     * @param e
     */
    void onComplete(ConsumerRecord record, Exception e) throws Exception;

    /**
     * 回调接口回收前的动作
     * @throws Exception
     */
    void cleanup() throws Exception;
}
