package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.framework.concurrent.ExecutionContext;
import org.kin.framework.utils.ClassUtils;
import org.kin.kafka.consumer.multi.utils.KafkaUtils;
import org.kin.kafka.consumer.multi.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * one kafka consumer one thread
 * 主要适用于保证Offset原子消费,尽管是Consumer Rebalance,这个特性多线程不好做,无法保证当前处理的消息
 * 马上发送到broker并让新的Consumer感知到.
 * <p>
 * 保证了consumer rebalance时的exactly once语义
 * <p>
 * 缺点:多个消费者占用资源会更多,单线程处理消费者分配的分区消息,速度会较慢
 * <p>
 * Created by 健勤 on 2017/7/26.
 */
public class OCOTMultiProcessor<K, V> {
    private static final Logger log = LoggerFactory.getLogger(OCOTMultiProcessor.class);

    /** 配置 */
    private final KafkaFetchConfig<K, V> fetchConfig;
    /** 线程池 */
    private final ExecutionContext executionContext;
    /** kafka consumer 消费线程实例 */
    private final List<KafkaMessageHandlerRunner> runners = new ArrayList<>();

    public OCOTMultiProcessor(KafkaFetchConfig<K, V> fetchConfig) {
        this.fetchConfig = fetchConfig;
        this.executionContext = ExecutionContext.cache("OCOTMultiProcessor");
    }

    /**
     * start fetch message from kafka
     */
    public void runFetch() {
        for (int i = 0; i < fetchConfig.getParallelism(); i++) {
            KafkaMessageHandlerRunner runner = new KafkaMessageHandlerRunner(i);
            runners.add(runner);
            executionContext.execute(runner);
        }
        log.info("OCOTMultiProcessor with {} kafka consumer started", fetchConfig.getParallelism());
    }

    /**
     * terminate
     */
    public void shutdown() {
        for (KafkaMessageHandlerRunner processor : runners) {
            processor.stop();
        }

        executionContext.shutdown();

        log.info("OCOTMultiProcessor with {} kafka consumer terminated", fetchConfig.getParallelism());
    }

    //------------------------------------------------------------------------------------------------------------------------
    class KafkaMessageHandlerRunner implements Runnable {
        /** 唯一标识 */
        private final int runnerId;
        /** kafka consumer */
        private final KafkaConsumer<K, V> consumer;
        /** 标识runner是否stopped */
        private volatile boolean isStopped = false;
        /** key -> topicpartition value -> 最近处理完的offset */
        private Map<TopicPartition, Long> topicPartition2Offset = new HashMap<>();

        /** key -> topic, value -> kafka 消息处理 */
        private Map<String, KafkaMessageHandler<K, V>> topic2MessageHandler = new HashMap<>();
        /** key -> topic, value -> offset提交策略 */
        private Map<String, OffsetCommitStrategy<K, V>> topic2OffsetCommitStrategy = new HashMap<>();
        /** kafka consumer rebalance listener实现 */
        private AbstractConsumerRebalanceListener consumerRebalanceListener;

        private KafkaMessageHandlerRunner(int runnerId) {
            this.runnerId = runnerId;
            this.consumer = new KafkaConsumer<>(fetchConfig.toKafkaConfig());

            Class<? extends AbstractConsumerRebalanceListener> consumerRebalanceListenerClass = fetchConfig.getConsumerRebalanceListenerClass();
            if (consumerRebalanceListenerClass != null) {
                this.consumerRebalanceListener = ClassUtils.instance(consumerRebalanceListenerClass, this);
            }
        }

        /**
         * 定位topic partition 的最近提交的Offset
         */
        public Long position(TopicPartition topicPartition) {
            return consumer.position(topicPartition);
        }

        /**
         * 移动到指定Offset
         */
        public void seekTo(TopicPartition topicPartition, Long offset) {
            consumer.seek(topicPartition, offset);
        }

        /**
         * 提交最近处理完的offset
         */
        public void commitOffset() {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(topicPartition2Offset.size());
            for (Map.Entry<TopicPartition, Long> entry : topicPartition2Offset.entrySet()) {
                offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue() + 1));
            }

            consumer.commitSync(offsets);
            topicPartition2Offset.clear();

            log.info("runner-{} consumer offsets [{}] committed", runnerId, TPStrUtils.topicPartitionOffsetsStr(offsets));
        }

        /**
         * stop
         */
        public void stop() {
            isStopped = true;
        }

        /**
         * 初始化
         */
        private void init() {
            if (Objects.nonNull(consumerRebalanceListener)) {
                consumerRebalanceListener.setup();
                consumer.subscribe(fetchConfig.topics(), this.consumerRebalanceListener);
            } else {
                consumer.subscribe(fetchConfig.topics());
            }

            //设置Kafka consumer某些分区开始消费的Offset
            KafkaUtils.setupKafkaStartOffset(consumer, fetchConfig.getOffset());
        }

        /**
         * 获取topic对应的KafkaMessageHandler, 如果没有, 则初始化一个
         */
        private KafkaMessageHandler<K, V> getOrCreateMessageHandler(String topic) {
            KafkaMessageHandler<K, V> messageHandler = topic2MessageHandler.get(topic);
            if (Objects.isNull(messageHandler)) {
                messageHandler = fetchConfig.constructMessageHandler(topic);
                messageHandler.setup(fetchConfig);
                topic2MessageHandler.put(topic, messageHandler);
            }

            return messageHandler;
        }

        /**
         * 获取topic对应的OffsetCommitStrategy, 如果没有, 则初始化一个
         */
        private OffsetCommitStrategy<K, V> getOrCreateOffsetCommitStrategy(String topic) {
            OffsetCommitStrategy<K, V> offsetCommitStrategy = topic2OffsetCommitStrategy.get(topic);
            if (Objects.isNull(offsetCommitStrategy)) {
                offsetCommitStrategy = fetchConfig.constructCommitStrategy(topic);
                offsetCommitStrategy.setup(fetchConfig);
                topic2OffsetCommitStrategy.put(topic, offsetCommitStrategy);
            }

            return offsetCommitStrategy;
        }

        /**
         * 消息处理
         */
        private void handle(ConsumerRecord<K, V> record) {
            try {
                getOrCreateMessageHandler(record.topic()).handle(record);
            } catch (Exception e) {
                //异常, 直接结束消息处理
                stop();
            }
        }

        /**
         * 尝试提交Offset
         */
        private void tryCommitOffset(ConsumerRecord<K, V> record) {
            String topic = record.topic();
            if (getOrCreateOffsetCommitStrategy(topic).isToCommit(getOrCreateMessageHandler(topic), record)) {
                commitOffset();
            }
        }

        @Override
        public void run() {
            init();

            log.info("runner-{} start fetch message", runnerId);
            try {
                while (!isStopped) {
                    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(fetchConfig.getPollTimeout()));

                    log.debug("runner-{} receive {} messages", runnerId, records.count());
                    for (ConsumerRecord<K, V> record : records) {
                        //存在可能消息积压在此处,调用close后可能会阻塞在这里,因此,两次判断isStopped标识来确保调用close后在进行关闭动作
                        if (isStopped) {
                            break;
                        }
                        //消息处理
                        handle(record);

                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        long offset = record.offset();
                        //记录某分区最新处理的ConsumerRecord
                        if (topicPartition2Offset.containsKey(topicPartition)) {
                            if (offset > topicPartition2Offset.get(topicPartition)) {
                                topicPartition2Offset.put(topicPartition, offset);
                            }
                        } else {
                            topicPartition2Offset.put(topicPartition, offset);
                        }

                        //判断是否需要commit offset
                        if (!fetchConfig.isAutoCommit()) {
                            tryCommitOffset(record);
                        }
                    }

                }
            } finally {
                //释放资源
                for (KafkaMessageHandler<K, V> messageHandler : topic2MessageHandler.values()) {
                    messageHandler.shutdown();
                }

                for (OffsetCommitStrategy<K, V> commitStrategy : topic2OffsetCommitStrategy.values()) {
                    commitStrategy.shutdown();
                }

                if (Objects.nonNull(consumerRebalanceListener)) {
                    consumerRebalanceListener.cleanup();
                }

                //提交最新的offset
                commitOffset();
                //close kafka consumer
                consumer.close();
            }
            log.info("runner-{} terminated", runnerId);
        }

        //getter
        public int getRunnerId() {
            return runnerId;
        }
    }
}
