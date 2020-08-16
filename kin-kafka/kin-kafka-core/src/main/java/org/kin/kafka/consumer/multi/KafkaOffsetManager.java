package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.framework.utils.CollectionUtils;
import org.kin.kafka.consumer.multi.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理kafka consumer offset
 *
 * @author huangjianqin
 * @date 2020/8/14
 */
class KafkaOffsetManager {
    private static final Logger log = LoggerFactory.getLogger(KafkaOffsetManager.class);
    /** 配置 */
    private final KafkaFetchConfig fetchConfig;
    /** kafka client */
    private final KafkaConsumer consumer;
    /**
     * 等待提交的Offset
     * 用Map的原因是如果同一时间内队列中有相同的topic分区的offset需要提交，那么map会覆盖原有的
     */
    private ConcurrentHashMap<TopicPartition, OffsetAndMetadata> pendingOffsets = new ConcurrentHashMap();

    /** 当前尝试commit offset重试次数 */
    private int nowRetry = 0;

    public KafkaOffsetManager(KafkaFetchConfig fetchConfig, KafkaConsumer consumer) {
        this.fetchConfig = fetchConfig;
        this.consumer = consumer;
    }

    void pushOffset(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        synchronized (pendingOffsets) {
            pendingOffsets.put(topicPartition, offsetAndMetadata);
        }
    }

    /**
     * sync commit offset
     */
    void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (CollectionUtils.isNonEmpty(offsets)) {
            return;
        }

        log.info("consumer commit offsets Sync...");
        consumer.commitSync(offsets);
        log.info("consumer offsets [{}] committed", TPStrUtils.topicPartitionOffsetsStr(offsets));
    }

    /**
     * 异步commit offset
     *
     * @param fallback 提交失败回调
     */
    void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, Runnable fallback) {
        if (CollectionUtils.isNonEmpty(offsets)) {
            return;
        }

        log.info("consumer commit offsets [{}] async", TPStrUtils.topicPartitionOffsetsStr(offsets));
        consumer.commitAsync(offsets, (Map<TopicPartition, OffsetAndMetadata> map, Exception e) -> {
            //Exception e --> The exception thrown during processing of the request, or null if the commit completed successfully
            if (e != null) {
                log.info("consumer commit offsets {} times failed!!!", nowRetry);
                //失败
                if (fetchConfig.enableRetry()) {
                    //允许重试,再次重新提交offset
                    if (nowRetry < fetchConfig.getMaxRetry()) {
                        log.info("consumer retry commit offsets");
                        commitOffsetsAsync(offsets, fallback);
                        nowRetry++;
                    } else {
                        log.error("consumer retry times greater than {} times(MaxRetry times)", fetchConfig.getMaxRetry());
                        fallback.run();
                    }
                } else {
                    //不允许,直接关闭该Fetcher
                    log.error("consumer disable retry commit offsets");
                    fallback.run();
                }
            } else {
                //成功,打日志
                nowRetry = 0;
                log.info("consumer offsets [{}] committed", TPStrUtils.topicPartitionOffsetsStr(offsets));
            }
        });
    }

    /**
     * 所有待提交的Offset
     */
    Map<TopicPartition, OffsetAndMetadata> allPendingOffsets() {
        if (CollectionUtils.isNonEmpty(pendingOffsets)) {
            synchronized (pendingOffsets) {
                if (CollectionUtils.isNonEmpty(pendingOffsets)) {
                    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : pendingOffsets.entrySet()) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                    pendingOffsets = new ConcurrentHashMap<>();
                    return result;
                }
            }
        }
        return null;
    }

    Map<TopicPartition, OffsetAndMetadata> allPendingOffsets(Collection<TopicPartition> topicPartitions) {
        if (CollectionUtils.isNonEmpty(pendingOffsets)) {
            synchronized (pendingOffsets) {
                if (CollectionUtils.isNonEmpty(pendingOffsets)) {
                    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
                    for (TopicPartition topicPartition : topicPartitions) {
                        if (pendingOffsets.contains(topicPartition)) {
                            result.put(topicPartition, pendingOffsets.get(topicPartition));
                            pendingOffsets.remove(topicPartition);
                        }
                    }
                    return result;
                }
            }
        }
        return null;
    }

    /**
     * 提交所有待提交的Offset
     */
    void flushOffset() {
        commitOffsets(allPendingOffsets());
    }

    /**
     * 提交指定topic partition待提交的Offset
     */
    void flushOffset(Collection<TopicPartition> topicPartitions) {
        commitOffsets(allPendingOffsets(topicPartitions));
    }
}
