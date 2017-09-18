package org.kin.kafka.multithread.api;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * Created by hjq on 2017/6/19.
 * Offset提交策略,只要规则通过才提交相应的Offset
 */
public interface CommitStrategy {
    /**
     * 初始化
     * @throws Exception
     */
    void setup(Properties config) throws Exception;

    /**
     * 判断是否满足自定义规则,满足则返回true
     * @param record
     * @return
     */
    boolean isToCommit(MessageHandler messageHandler, ConsumerRecord record);

    /**
     * 重置规则
     */
    void reset();
    /**
     *
     * @throws Exception
     */
    void cleanup() throws Exception;

}
