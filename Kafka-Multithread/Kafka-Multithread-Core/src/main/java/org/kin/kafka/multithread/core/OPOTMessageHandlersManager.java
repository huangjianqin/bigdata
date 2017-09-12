package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.MessageHandler;
import org.kin.kafka.multithread.api.CommitStrategy;
import org.kin.kafka.multithread.utils.ConsumerRecordInfo;
import org.kin.kafka.multithread.utils.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by hjq on 2017/6/19.
 * OPOT ==> one partition one Thread
 */
public class OPOTMessageHandlersManager extends AbstractMessageHandlersManager{
    private static final Logger log = LoggerFactory.getLogger(OPOTMessageHandlersManager.class);
    private Map<TopicPartition, OPOTMessageQueueHandlerThread> topicPartition2Thread = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor threads = new ThreadPoolExecutor(2, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets){
        log.debug("dispatching message: " + StrUtils.consumerRecordDetail(consumerRecordInfo.record()));

        while(isRebalance.get()){
            try {
                Thread.currentThread().sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();
        if(topicPartition2Thread.containsKey(topicPartition)){
            //已有该topic分区对应的线程启动
            //直接添加队列
            topicPartition2Thread.get(topicPartition).queue().add(consumerRecordInfo);
            log.debug("message: " + StrUtils.consumerRecordDetail(consumerRecordInfo.record()) + "queued(" + topicPartition2Thread.get(topicPartition).queue().size() + " rest)");
        }
        else{
            //没有该topic分区对应的线程'
            //先启动线程,再添加至队列
            OPOTMessageQueueHandlerThread thread = newThread(pendingOffsets, topicPartition.topic() + "-" + topicPartition.partition(), newMessageHandler(topicPartition.topic()), newCommitStrategy(topicPartition.topic()));
            topicPartition2Thread.put(topicPartition, thread);
            thread.queue().add(consumerRecordInfo);
            runThread(thread);
            log.debug("message: " + StrUtils.consumerRecordDetail(consumerRecordInfo.record()) + "queued(" + thread.queue.size() + " rest)");
        }

        return true;
    }

    @Override
    public void consumerCloseNotify(){
        log.info("shutdown all handlers...");
        //停止所有handler
        for(OPOTMessageQueueHandlerThread thread: topicPartition2Thread.values()){
            thread.stop();
        }

        //等待所有handler完成,超过10s,强制关闭
        boolean isTimeOut = waitingThreadPoolIdle((Collection)topicPartition2Thread.values(), 10000);

        //关闭线程池
        if(!isTimeOut){
            log.info("shutdown thread pool...");
            threads.shutdown();
        }
        else{
            log.warn("force shutdown thread pool...");
            threads.shutdownNow();
        }
        log.info("thread pool terminated");
    }

    /**
     * 之前分配到的TopicPartitions
     * @param topicPartitions 当前分配到的分区
     */
    @Override
    public void consumerRebalanceNotify(Set<TopicPartition> topicPartitions) {
        isRebalance.set(true);

        //提交所有message handler最新处理的Offset
        //插入待提交队列
        for(OPOTMessageQueueHandlerThread thread: topicPartition2Thread.values()){
            thread.commitLatest();
        }
    }

    /**
     * @param topicPartitions 之前分配到但此次又没有分配到的TopicPartitions
     */
    @Override
    public void doOnConsumerReAssigned(Set<TopicPartition> topicPartitions) {
        log.info("clean up handlers(not thread)");

        if(topicPartitions.size() < 0){
            return;
        }

        //关闭没有分配但是之前分配了的message handler
        Set<OPOTMessageQueueHandlerThread> messageQueueHandlerThreads = new HashSet<>();
        for(TopicPartition topicPartition: topicPartitions){
            if(topicPartition2Thread.containsKey(topicPartition)){
                topicPartition2Thread.get(topicPartition).stop();
                //清除没有分配但是之前分配了的分区
                topicPartition2Thread.remove(topicPartition);
            }
        }

        //等待线程池中线程空闲,如果超过3s,则抛异常,并释放资源
        boolean isTimeOut = waitingThreadPoolIdle((Collection)messageQueueHandlerThreads, 3000);
        if(isTimeOut){
            log.warn("waiting for target message handlers terminated timeout when rebalancing!!!");
            System.exit(-1);
        }

        isRebalance.set(false);
    }

    private OPOTMessageQueueHandlerThread newThread(Map<TopicPartition, OffsetAndMetadata> pendingOffsets, String logHead, MessageHandler messageHandler, CommitStrategy commitStrategy){
        return new OPOTMessageQueueHandlerThread(logHead, pendingOffsets, messageHandler, commitStrategy);
    }

    private void runThread(Runnable target){
        threads.submit(target);
    }

    private final class OPOTMessageQueueHandlerThread extends AbstractMessageHandlersManager.MessageQueueHandlerThread {

        public OPOTMessageQueueHandlerThread(String LOG_HEAD, Map<TopicPartition, OffsetAndMetadata> pendingOffsets, MessageHandler messageHandler, CommitStrategy commitStrategy) {
            super(LOG_HEAD, pendingOffsets, messageHandler, commitStrategy);
        }

        @Override
        protected void preTerminated() {
            //只有两种情况:
            //1:
            //  1.1rebalance consumerRebalanceNotify先手动提交最新Offset
            //  1.2doOnConsumerReAssigned再一次提交之前分配到但此次又没有分配到的TopicPartition对应的最新Offset
            //2:关闭consumer 抛弃这些待处理信息,提交最近处理的offset
            log.info(LOG_HEAD() + " closing/rebalancing consumer should commit last offsets sync now");
            if(lastRecord != null){
                pendingOffsets.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()), new OffsetAndMetadata(lastRecord.offset() + 1));
            }
            super.preTerminated();
        }

    }
}
