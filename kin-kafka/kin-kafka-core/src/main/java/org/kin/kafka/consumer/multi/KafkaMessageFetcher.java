package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.kin.framework.concurrent.Keeper;
import org.kin.framework.concurrent.KeeperAction;
import org.kin.framework.utils.CollectionUtils;
import org.kin.kafka.consumer.multi.utils.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * 负责抓取信息的线程
 * Created by hjq on 2017/6/19.
 */
public class KafkaMessageFetcher<K, V> {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageFetcher.class);

    /** 配置 */
    private final KafkaFetchConfig<K, V> fetchConfig;
    /** kafka client */
    private final KafkaConsumer<K, V> consumer;
    /** kafka offset管理 */
    private final KafkaOffsetManager kafkaOffsetManager;

    /** 消息批量处理 */
    private OPMTMultiProcessor<K, V> handlersManager;

    /** 标识fetcher是否stopped */
    private volatile boolean isStopped = false;
    /** stop fetch */
    private volatile Keeper.KeeperStopper fetcherStopper;

    public KafkaMessageFetcher(KafkaFetchConfig<K, V> fetchConfig) {
        this.fetchConfig = fetchConfig;
        this.handlersManager = new OPMTMultiProcessor<>(fetchConfig);
        this.consumer = new KafkaConsumer<>(fetchConfig.toKafkaConfig());
        this.consumer.subscribe(fetchConfig.topics(), new InnerConsumerRebalanceListener());
        this.kafkaOffsetManager = new KafkaOffsetManager(fetchConfig, consumer);
    }

    /**
     * start fetch message from kafka
     */
    public void runFetch() {
        if (isStopped) {
            return;
        }
        //设置Kafka consumer某些分区开始消费的Offset
        KafkaUtils.setupKafkaStartOffset(consumer, fetchConfig.getOffset());

        fetcherStopper = Keeper.keep(new FetchKeeper());
    }

    /**
     * terminate
     */
    public void shutdown() {
        if (Objects.isNull(fetcherStopper)) {
            return;
        }
        if (isStopped) {
            return;
        }
        log.info("consumer fetcher thread closing...");
        isStopped = true;
        fetcherStopper.stop();
    }

    //--------------------------------------------------------------------------------------------------------------------------
    private class FetchKeeper implements KeeperAction {
        /** 统计空闲次数 */
        private long idleCounter = 0;
        private long offset = -1;

        @Override
        public void preAction() {
            log.info("consumer fetcher started");
        }

        @Override
        public void action() {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(fetchConfig.getPollTimeout()));

            //空闲n久就开始输出.....
            if (records == null || records.count() < 0) {
                idleCounter++;
                if (idleCounter > fetchConfig.getIdleTimesAlarm()) {
                    log.warn("{} round not receive message from kafka", idleCounter);
                }
                return;
            } else {
                idleCounter = 0;
            }

            for (TopicPartition topicPartition : records.partitions()) {
                for (ConsumerRecord<K, V> record : records.records(topicPartition)) {
                    if (record.offset() > offset) {
                        offset = record.offset();
                    }
                    handlersManager.dispatch(new KafkaMessageWrapper<>(record), kafkaOffsetManager);
                }
            }
        }

        @Override
        public void postAction() {
            //等待所有该消费者接受的消息处理完成
            handlersManager.consumerCloseNotify();
            //关闭前,提交所有offset
            kafkaOffsetManager.flushOffset();
            consumer.close();
            log.info("kafka conusmer closed");
            log.info("kafka consumer message fetcher closed");
            log.info("kafka comsumer commit max offset '{}'", offset);
        }
    }

    /**
     * 在poll时调用,会停止receive 消息
     * <p>
     * 并不能保证多个实例不会重复消费消息
     * 感觉只有每个线程一个消费者才能做到,不然消费了的消息很难做到及时提交Offset并且其他实例还没有启动
     * <p>
     * 该监听器的主要目的是释放那些无用资源
     */
    private class InnerConsumerRebalanceListener implements ConsumerRebalanceListener {
        /** 之前分配到的TopicPartition */
        private volatile Set<TopicPartition> beforeAssignedTopicPartition;
        /** jvm内存缓存目前消费到的Offset **/
        private final Map<TopicPartition, Long> topicPartition2Offset = new HashMap<>();

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            log.info("kafka consumer rebalanced");
            //刚启动
            if (CollectionUtils.isEmpty(collection)) {
                return;
            }

            //设置标识,禁止message fetcher dispatch 消息
            handlersManager.consumerRebalanceNotify(new HashSet<>(collection));
            //保存之前分配到的TopicPartition
            beforeAssignedTopicPartition = new HashSet<>(collection);
            //提交最新处理完的Offset
            kafkaOffsetManager.flushOffset();

            //缓存当前Consumer poll到的topic partition Offset
            //保存在jvm内存中
            StringBuffer sb = new StringBuffer();
            for (TopicPartition topicPartition : collection) {
                long offset = consumer.position(topicPartition);
                topicPartition2Offset.put(topicPartition, offset);
                sb.append(topicPartition.toString().concat(":").concat(Long.toString(offset)).concat(","));
            }
            sb.deleteCharAt(sb.length() - 1);
            log.info("kafka topic partition origin offset >>> {}", sb.toString());
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            log.info("kafka consumer onPartitionsAssigned");

            //获取之前分配到但此次又没有分配到的TopicPartitions
            Set<TopicPartition> invalidTopicPartitions = new HashSet<>(beforeAssignedTopicPartition);
            invalidTopicPartitions.removeAll(collection);
            //还需清理已经交给handler线程
            handlersManager.consumerReassigned(invalidTopicPartitions);
            //再一次提交之前分配到但此次又没有分配到的TopicPartition对应的最新Offset
            kafkaOffsetManager.flushOffset(invalidTopicPartitions);
            beforeAssignedTopicPartition = null;

            //重置consumer position并reset缓存
            if (CollectionUtils.isNonEmpty(collection)) {
                StringBuffer sb = new StringBuffer();
                for (TopicPartition topicPartition : collection) {
                    Long originOffset = topicPartition2Offset.get(topicPartition);
                    if (Objects.nonNull(originOffset)) {
                        consumer.seek(topicPartition, originOffset);
                        sb.append(topicPartition.toString().concat(":").concat(Long.toString(originOffset)).concat(","));
                    } else {
                        sb.append(topicPartition.toString().concat(":latest,"));
                    }
                }
                sb.deleteCharAt(sb.length() - 1);
                log.info("kafka consumer begin fetcher message from >>> {}", sb.toString());
                //清理offset缓存
                topicPartition2Offset.clear();
            }
        }

    }

    //getter
    public Set<TopicPartition> getSubscribed() {
        return consumer.assignment();
    }
}
