package org.kin.kafka.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.kin.framework.concurrent.ExecutionContext;
import org.kin.framework.concurrent.Keeper;
import org.kin.framework.utils.CollectionUtils;
import org.kin.framework.utils.HashUtils;
import org.kin.kafka.consumer.multi.utils.TPStrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * one topic partition more thread
 * 如果parallelism设置成1, 也就变成one topic partition one thread
 * <p>
 * 基于滑动窗口, 主要控制offset提交速率, 并且高效保证offset提交
 * <p>
 * Created by 健勤 on 2017/7/18.
 */
public class OPMTMultiProcessor<K, V> {
    private static final Logger log = LoggerFactory.getLogger(OPMTMultiProcessor.class);
    /** 配置 */
    private final KafkaFetchConfig<K, V> fetchConfig;
    /** key -> topic partition, value -> 滑动窗口 */
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new ConcurrentHashMap<>();
    /**
     * key -> topic partition, value -> 消息处理线程
     * lazy init
     */
    private Map<TopicPartition, List<KafkaMessageHandlerRunner>> topicPartition2Runners = new ConcurrentHashMap<>();
    /** 消息处理线程池 */
    private ExecutionContext executionContext = ExecutionContext.cache("OPMTMultiProcessor");
    /**
     * 定期更新pendingwindow, Offset过长都留在pendingwindow中
     * <p>
     * 因为PendingWindow的Offset commit更新操作是在{@link KafkaMessageHandlerRunner}处理,如果PendingWindow缓存着大量待提交的Offset
     * 如果此时出现系统故障,大量完成的record因为没有提交Offset而需要重新处理
     * 所以通过定时抢占来完成PendingWindow的Offset commit更新操作以减少不必要的record重复处理
     */
    private Keeper.KeeperStopper updatePendingWindowKeeper;
    /**
     * 标识kafak consumer rebalance
     */
    private volatile boolean isRebalance = false;

    public OPMTMultiProcessor(KafkaFetchConfig<K, V> fetchConfig) {
        this.fetchConfig = fetchConfig;
    }

    /**
     * 更新pendingwindow(10s)
     */
    private void updatePendingWindow() {
        long sleep = 10000 - System.currentTimeMillis() % 1000;
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {

        }

        //不是Rebalance的时候才更新pendingwindow
        if (!isRebalance) {
            for (PendingWindow pendingWindow : topicPartition2PendingWindow.values()) {
                pendingWindow.commitFinished(null);
            }
        }
    }

    /**
     * dispatch 消息
     * fetcher线程执行
     */
    public boolean dispatch(KafkaMessageWrapper<K, V> kafkaMessageWrapper, KafkaOffsetManager kafkaOffsetManager) {
        log.debug("dispatching message: " + TPStrUtils.consumerRecordDetail(kafkaMessageWrapper.record()));

        //自动提交的情况下,不会使用pendingwindow,进而没必要启动一条定时线程来检索空的map实例
        if (!fetchConfig.isAutoCommit() && Objects.isNull(updatePendingWindowKeeper)) {
            updatePendingWindowKeeper = Keeper.keep(this::updatePendingWindow);
        }

        if (isRebalance) {
            log.debug("dispatch failure rebalancing...");
            return false;
        }

        TopicPartition topicPartition = kafkaMessageWrapper.topicPartition();
        List<KafkaMessageHandlerRunner> runners;
        KafkaMessageHandlerRunner selectedRunner;
        PendingWindow pendingWindow = topicPartition2PendingWindow.get(topicPartition);

        runners = topicPartition2Runners.get(topicPartition);
        if (CollectionUtils.isEmpty(runners)) {
            //没有该topic分区对应的线程池
            //先启动线程池,再添加至队列
            if (!fetchConfig.isAutoCommit() && pendingWindow == null) {
                pendingWindow = new PendingWindow(fetchConfig.getWindowSize(), kafkaOffsetManager);
                topicPartition2PendingWindow.put(topicPartition, pendingWindow);
            }
            runners = new ArrayList<>();
            log.info("init [{}] message handler runners for topic-partition({})", fetchConfig.getParallelism(), topicPartition.toString());
            for (int i = 0; i < fetchConfig.getParallelism(); i++) {
                KafkaMessageHandlerRunner runner =
                        new KafkaMessageHandlerRunner(
                                topicPartition.toString().concat("#").concat(Integer.toString(i)),
                                kafkaOffsetManager,
                                fetchConfig.constructMessageHandler(topicPartition.topic()),
                                null,
                                pendingWindow);
                runners.add(runner);
                executionContext.execute(runner);
            }
            topicPartition2Runners.put(topicPartition, runners);
        }
        selectedRunner = runners.get(HashUtils.efficientHash(kafkaMessageWrapper.record(), runners.size()));

        if (Objects.nonNull(selectedRunner)) {
            selectedRunner.push(kafkaMessageWrapper);
            log.debug("message: {} queued({} rest)", TPStrUtils.consumerRecordDetail(kafkaMessageWrapper.record()), selectedRunner.queue.size());
            return true;
        } else {
            log.error("can't find suitable KafkaMessageHandlerRunner >>>> {}", TPStrUtils.consumerRecordDetail(kafkaMessageWrapper.record()));
            return false;
        }
    }

    /**
     * kafka consumer关闭时触发
     */
    public void consumerCloseNotify() {
        if (Objects.nonNull(updatePendingWindowKeeper)) {
            updatePendingWindowKeeper.stop();
        }

        List<KafkaMessageHandlerRunner> allRunners = new ArrayList<>();
        //停止所有handler
        for (List<KafkaMessageHandlerRunner> runners : topicPartition2Runners.values()) {
            for (KafkaMessageHandlerRunner runner : runners) {
                runner.stop();
                allRunners.add(runner);
            }
        }

        executionContext.shutdown();

        log.info("OPMTMultiProcessor closed");
    }

    /**
     * topic partition consumer rebalance时触发
     * 之前分配到的TopicPartitions
     *
     * @param topicPartitions 当前分配到的分区
     */
    public void consumerRebalanceNotify(Set<TopicPartition> topicPartitions) {
        isRebalance = true;

        for (PendingWindow pendingWindow : topicPartition2PendingWindow.values()) {
            //提交已完成处理的消息的最大offset
            pendingWindow.commitLatest();
        }
    }

    /**
     * topic partition consumer reassigned时触发
     * <p>
     * 主要是清理负责之前分配到但此次又没有分配到的TopicPartition对应的runner及一些资源
     * 新分配到的TopicPartition(之前没有的)由dispatch方法新分配资源
     *
     * @param invalidTopicPartitions 之前分配到但此次又没有分配到的TopicPartitions
     */
    public void consumerReassigned(Set<TopicPartition> invalidTopicPartitions) {
        List<KafkaMessageHandlerRunner> allThreads = new ArrayList<>();
        //关闭Handler执行,但不关闭线程,达到线程复用的效果
        for (TopicPartition topicPartition : invalidTopicPartitions) {
            for (KafkaMessageHandlerRunner runner : topicPartition2Runners.get(topicPartition)) {
                runner.stop();
            }
            //移除属于该分区的runner
            allThreads.addAll(topicPartition2Runners.remove(topicPartition));
        }

        //等待runner stopped, 如果超过3s,则直接释放资源
        boolean isTimeOut = waitingThreadPoolIdle(allThreads, 3000);
        if (isTimeOut) {
            log.warn("waiting for target message handlers terminated timeout when rebalancing!!!");
        }

        isRebalance = false;
    }

    private boolean checkHandlerTerminated() {
        for (List<KafkaMessageHandlerRunner> threads : topicPartition2Runners.values()) {
            for (KafkaMessageHandlerRunner thread : threads) {
                if (!thread.isStooped) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 等待指定 runners stopped
     * 如果超时返回true,否则返回false
     */
    private boolean waitingThreadPoolIdle(Collection<KafkaMessageHandlerRunner> kafkaMessageHandlerRunners, long timeout) {
        if (CollectionUtils.isEmpty(kafkaMessageHandlerRunners)) {
            return false;
        }
        if (timeout < 0) {
            throw new IllegalStateException("timeout should be greater than 0(now is " + timeout + ")");
        }
        long baseTime = System.currentTimeMillis();
        while (!checkHandlerTerminated()) {
            if (System.currentTimeMillis() - baseTime > timeout) {
                log.warn("target message handlers terminate time out!!!");
                return true;
            }
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
            }
        }

        return false;
    }

    //-------------------------------------------------------------------------------------------------------------------

    /**
     * 消息处理线程实现
     */
    private class KafkaMessageHandlerRunner implements Runnable {
        /** runner标识 */
        private final String runnerId;
        /** kafka Offset 管理 */
        private final KafkaOffsetManager offsetManager;
        /** 消息处理器 */
        private final KafkaMessageHandler<K, V> kafkaMessageHandler;
        /** Offset提交策略 */
        private final OffsetCommitStrategy offsetCommitStrategy;
        /** 滑动窗口 */
        private final PendingWindow pendingWindow;
        /** 按消息插入顺序排序 */
        private final LinkedBlockingQueue<KafkaMessageWrapper<K, V>> queue = new LinkedBlockingQueue<>();
        /** 标识runner是否stopped */
        protected volatile boolean isStooped = false;
        /** 最近一次处理的最大的offset */
        protected ConsumerRecord<K, V> lastRecord = null;

        public KafkaMessageHandlerRunner(String runnerId,
                                         KafkaOffsetManager offsetManager,
                                         KafkaMessageHandler<K, V> kafkaMessageHandler,
                                         OffsetCommitStrategy offsetCommitStrategy,
                                         PendingWindow pendingWindow) {
            this.runnerId = runnerId;
            this.offsetManager = offsetManager;
            this.kafkaMessageHandler = kafkaMessageHandler;
            this.offsetCommitStrategy = offsetCommitStrategy;
            this.pendingWindow = pendingWindow;
        }

        /**
         * 消息入队
         */
        public void push(KafkaMessageWrapper<K, V> messageWrapper) {
            if (isStooped) {
                return;
            }
            queue.add(messageWrapper);
        }

        /**
         * stop
         */
        public void stop() {
            log.info(runnerId + " stopping...");
            this.isStooped = true;
        }

        /**
         * 处理消息
         */
        private boolean handle(KafkaMessageWrapper<K, V> wrapper) {
            //对Kafka消息的处理
            boolean success = handleKafkaMessage(wrapper);

            if (!success) {
                //异常直接break
                return false;
            }

            //保存最新的offset
            if (Objects.nonNull(lastRecord)) {
                if (wrapper.record().offset() > lastRecord.offset()) {
                    lastRecord = wrapper.record();
                }
            } else {
                lastRecord = wrapper.record();
            }

            if (!fetchConfig.isAutoCommit()) {
                tryCommitOffset(wrapper);
            }

            return true;
        }

        /**
         * 真正对kafka消息进行处理
         */
        private boolean handleKafkaMessage(KafkaMessageWrapper<K, V> wrapper) {
            try {
                kafkaMessageHandler.handle(wrapper.record());
                return true;
            } catch (Exception e) {
                //异常, stopped
                stop();
                return false;
            }
        }

        /**
         * 尝试提交Offset
         */
        private void tryCommitOffset(KafkaMessageWrapper<K, V> wrapper) {
            pendingWindow.commitFinished(wrapper);
        }

        @Override
        public void run() {
            log.info(runnerId + " start up");

            try {
                while (!this.isStooped) {
                    KafkaMessageWrapper<K, V> wrapper = queue.poll();
                    //队列中有消息需要处理
                    if (Objects.nonNull(wrapper)) {
                        boolean success = handle(wrapper);
                        if (!success) {
                            break;
                        }
                    }
                }
            } finally {
                //队列里面还未处理的消息, 则不管, 等待下次启动, 再次从上次提交的offset位置开始消息
                //把所有最新的处理完的Offset刷到offset manager
                pendingWindow.commitLatest();
                //提交所有最新的处理完的Offset
                offsetManager.flushOffset();

                //释放message handler和CommitStrategy的资源
                try {
                    if (offsetCommitStrategy != null) {
                        offsetCommitStrategy.shutdown();
                    }
                    if (kafkaMessageHandler != null) {
                        kafkaMessageHandler.shutdown();
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
                log.info(runnerId + " terminated");
            }
        }
    }
}
