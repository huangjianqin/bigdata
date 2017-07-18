package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by hjq on 2017/6/19.
 * OPOT ==> one partition one Thread
 * 相对而言,小部分实例都是长期存在的,大部分实例属于新生代(kafka的消费实例,因为很多,所以占据大部分,以致核心对象实例只占据一小部分)
 * 可考虑增加新生代的大小来减少GC的消耗
 */
public class OPOTMessageHandlersManager extends AbstractMessageHandlersManager{
    private static Logger log = LoggerFactory.getLogger(OPOTMessageHandlersManager.class);
    private Map<TopicPartition, OPOTMessageQueueHandlerThread> topicPartition2Thread = new ConcurrentHashMap<>();
    private ThreadPoolExecutor threads = new ThreadPoolExecutor(2, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets){
        log.debug("dispatching message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()));

        if(isRebalance.get()){
            log.debug("dispatch failure ~~~ rebalancing...");
            return false;
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();
        if(topicPartition2Thread.containsKey(topicPartition)){
            //已有该topic分区对应的线程启动
            //直接添加队列
            topicPartition2Thread.get(topicPartition).queue().add(consumerRecordInfo);
            log.debug("message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()) + "queued(" + topicPartition2Thread.get(topicPartition).queue().size() + " rest)");
        }
        else{
            //没有该topic分区对应的线程'
            //先启动线程,再添加至队列
            OPOTMessageQueueHandlerThread thread = newThread(pendingOffsets, topicPartition.topic() + "-" + topicPartition.partition());
            topicPartition2Thread.put(topicPartition, thread);
            thread.queue.add(consumerRecordInfo);
            runThread(thread);
            log.debug("message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()) + "queued(" + thread.queue.size() + " rest)");
        }

        return true;
    }

    private boolean checkHandlerTerminated(){
        for(OPOTMessageQueueHandlerThread thread: topicPartition2Thread.values()){
            if(!thread.isTerminated()){
                return false;
            }
        }
        log.info("all handlers terminated");
        return true;
    }

    @Override
    public void consumerCloseNotify(Set<TopicPartition> topicPartitions){
        log.info("shutdown all handlers...");
        //停止所有handler
        for(OPOTMessageQueueHandlerThread thread: topicPartition2Thread.values()){
            thread.close();
        }

        //等待所有handler完成,超过10s,强制关闭
        int count = 0;
        while(!checkHandlerTerminated() && count < 5){
            count ++;
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //关闭线程池
        if(count < 5){
            log.info("shutdown thread pool...");
            threads.shutdown();
        }
        else{
            log.info("force shutdown thread pool...");
            threads.shutdownNow();
        }
        log.info("thread pool terminated");

        cleanMsgHandlersAndCommitStrategies();

    }

    @Override
    public void consumerRebalanceNotify(){
        isRebalance.set(true);
        log.info("clean up handlers(not thread)");
        //防止有Handler线程提交offset到offset队列
        for(CommitStrategy commitStrategy: topic2CommitStrategy.values()){
            commitStrategy.reset();
        }

        //关闭Handler执行,但不关闭线程,达到线程复用的效果
        for(OPOTMessageQueueHandlerThread thread: topicPartition2Thread.values()){
            //不清除队列好像也可以
            thread.close();
        }

        //清楚topic分区与handler的映射
        topicPartition2Thread.clear();
        isRebalance.set(false);
    }

    private OPOTMessageQueueHandlerThread newThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets, String logHead){
        return new OPOTMessageQueueHandlerThread(pendingOffsets, logHead);
    }

    private void runThread(Runnable target){
        threads.submit(target);
    }

    private class OPOTMessageQueueHandlerThread extends AbstractMessageHandlersManager.MessageQueueHandlerThread {

        public OPOTMessageQueueHandlerThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets, String logHead) {
            super(logHead, pendingOffsets);
        }

        @Override
        protected void preTerminated() {
            //只有两种情况:
            //1:rebalance 抛弃这些待处理信息
            //2:关闭consumer 抛弃这些待处理信息,提交最近处理的offset
            if(!isRebalance.get()){
                log.info(LOG_HEAD() + " closing consumer should commit last offsets sync now");
                if(lastRecord != null){
                    pendingOffsets.put(new TopicPartitionWithTime(new TopicPartition(lastRecord.topic(), lastRecord.partition()),
                            System.currentTimeMillis()), new OffsetAndMetadata(lastRecord.offset() + 1));
                }
            }
            super.preTerminated();
        }

    }
}
