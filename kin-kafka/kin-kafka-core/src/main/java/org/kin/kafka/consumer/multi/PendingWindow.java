package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.framework.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 滑动窗口实现
 * <p>
 * Created by hjq on 2017/7/4.
 */
public class PendingWindow {
    private static final Logger log = LoggerFactory.getLogger(PendingWindow.class);
    /** kafka offset 管理 */
    private final KafkaOffsetManager offsetManager;
    /** offset队列, 即窗口 */
    private final ConcurrentSkipListSet<TopicPartitionOffset> queue;
    /** 窗口大小 */
    private final int windowSize;

    /**
     * 提交offset控制权
     * 标识是否有处理线程正在判断窗口满足连续性
     */
    private final AtomicBoolean commitControl = new AtomicBoolean(false);
    /**
     * 记录上次commit offset
     * 防止出现跳offset commit,也就是处理了提交了offset 1, 但是offset 345进入队列,而offset 2还没有完成处理时,直接提交offset 5的异常
     */
    private volatile long lastCommittedOffset = -1;

    public PendingWindow(int windowSize, KafkaOffsetManager offsetManager) {
        this.windowSize = windowSize;
        this.offsetManager = offsetManager;
        this.queue = new ConcurrentSkipListSet<>((o1, o2) -> {
            if (o2 == null) {
                return 1;
            }

            long offset1 = o1.offset();
            long offset2 = o2.offset();

            return Long.compare(offset1, offset2);
        });
    }

    /**
     * 清空窗口
     */
    public void clean() {
        log.debug("window clean up");
        queue.clear();
    }

    /**
     * 当record为空时,也就是说明当前线程想抢占并提交有效连续的Offset
     * 会有一条调度线程定时检测是否能提交，目的是保证PendingWindow不能长期拥有未提交的Offset
     */
    public <K, V> void commitFinished(KafkaMessageWrapper<K, V> record) {
        if (record != null) {
            log.debug("consumer record " + record.record() + " finished");

            TopicPartition topicPartition = record.topicPartition();
            queue.add(new TopicPartitionOffset(topicPartition.topic(), topicPartition.partition(), record.offset()));

            //有点像是拿来主义,选举出leader进行处理(先到先得)
            //保证同一时间只有一条处理线程判断窗口满足
            //多线判断窗口满足有点难,需要加锁,感觉性能还不如控制一次只有一条线程判断窗口满足,无锁操作queue,其余处理线程直接进队
            //原子操作设置标识
            if (queue.size() >= windowSize) {
                //队列大小满足窗口大小才去判断
                if (commitControl.compareAndSet(false, true)) {
                    //判断是否满足窗口,若满足,则提交Offset
                    commitLatest(true);
                    commitControl.set(false);
                }
            }
        } else {
            commitLatest();
        }
    }

    public void commitLatest() {
        //尝试抢占并更新提交最长连续的offset
        if (commitControl.compareAndSet(false, true)) {
            //判断是否满足窗口,若满足,则提交Offset
            commitLatest(false);
            commitControl.set(false);
        }
    }


    /**
     * 根据条件把连续的最大的offset提交到offset manager中, 待consumer 提交到 broker
     *
     * @param checkSize 标识提交时是否检查窗口大小是否满足, 如果需要检查, 满足窗口大小, 则直接把连续的最大的offset提交到offset manager中
     */
    private void commitLatest(boolean checkSize) {
        //队列为空,则返回
        if (CollectionUtils.isEmpty(queue)) {
            return;
        }

        //窗口大小都没满足,则返回
        if (checkSize && queue.size() < windowSize) {
            return;
        }

        //如果队列头与上一次提交的offset不连续,直接返回, 不提交有误offset
        long queueFirstOffset = queue.first().offset();
        if (lastCommittedOffset != -1 && queueFirstOffset - lastCommittedOffset != 1) {
            log.warn("last committed offset '{}' is not continued with now queue head '{}'", lastCommittedOffset, queueFirstOffset);
            return;
        }

        //复制视图
        TopicPartitionOffset[] queueView = new TopicPartitionOffset[queue.size()];
        //toArray方法是如果参数数组长度比queue少,会创建一个新的Array实例并返回
        queueView = queue.toArray(queueView);

        long maxOffset = queueView[0].offset();
        String topic = queueView[0].topic();
        int partition = queueView[0].partition();

        log.debug("try to commit largest continued finished consumer records offsets");
        //需要把处理完的连续的最新的Offset提交到offset manager
        for (int i = 1; i < queueView.length; i++) {
            long thisOffset = queueView[i].offset();
            if (queue.first().offset() != queueView[i - 1].offset()) {
                //offset不对应
                return;
            }
            //判断offset是否连续
            if (thisOffset - maxOffset == 1) {
                //连续
                maxOffset = thisOffset;
            } else {
                //非连续
                //如果是获取queue最大Offset,就直接退出循环
                break;
            }
        }

        //移除 <= maxoffset的所有offset记录
        while (queue.first().offset() <= maxOffset) {
            queue.pollFirst();
        }

        lastCommittedOffset = maxOffset;

        pendingToCommit(new TopicPartition(topic, partition), new OffsetAndMetadata(maxOffset + 1));
    }

    /**
     * 窗口Offset满足连续性, 提交到offset 管理, 准备提交到kafka broker
     */
    private void pendingToCommit(TopicPartition topicPartition, OffsetAndMetadata offset) {
        log.debug("pending to commit offset {}({})", offset, topicPartition);
        offsetManager.pushOffset(topicPartition, offset);
    }

    //--------------------------------------------------------------------------------------------------------------------------------
    private class TopicPartitionOffset {
        /** kafka topic */
        private final String topic;
        /** kafka topic partition */
        private final int partition;
        /** kafka topic partition offset */
        private final long offset;

        public TopicPartitionOffset(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        public String topic() {
            return topic;
        }

        public int partition() {
            return partition;
        }

        public long offset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TopicPartitionOffset that = (TopicPartitionOffset) o;
            return partition == that.partition &&
                    offset == that.offset &&
                    Objects.equals(topic, that.topic);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, offset);
        }
    }
}
