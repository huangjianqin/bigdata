package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 健勤 on 2017/7/18.
 * 基于OPOT思想的OPMT改进版本
 */
public class OPMTMessageHandlersManager2 extends AbstractMessageHandlersManager {
    private static Logger log = LoggerFactory.getLogger(OPMTMessageHandlersManager.class);
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new ConcurrentHashMap<>();
    private Map<TopicPartition, List<OPMTMessageQueueHandlerThread>> topicPartition2Threads = new ConcurrentHashMap<>();
    private ThreadPoolExecutor threads = new ThreadPoolExecutor(2, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets){
        log.debug("dispatching message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()));

        if(isRebalance.get()){
            log.debug("dispatch failure ~~~ rebalancing...");
            return false;
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();
        List<OPMTMessageQueueHandlerThread> threads = null;
        OPMTMessageQueueHandlerThread selectedThread = null;
        PendingWindow pendingWindow = topicPartition2PendingWindow.get(topicPartition);
        if(topicPartition2Threads.containsKey(topicPartition)){
            //已有该topic分区对应的线程启动
            //直接添加队列
            threads = topicPartition2Threads.get(topicPartition);
            selectedThread = threads.get(consumerRecordInfo.record().hashCode() % threads.size());
        }
        else{
            //没有该topic分区对应的线程
            //先启动线程,再添加至队列
            if(pendingWindow == null){
                pendingWindow = new PendingWindow(100000, pendingOffsets);
                topicPartition2PendingWindow.put(topicPartition, pendingWindow);
            }
            threads = new ArrayList<>();
            for(int i = 0; i < 2; i++){
                OPMTMessageQueueHandlerThread thread = newThread(pendingOffsets, topicPartition.topic() + "-" + topicPartition.partition() + "#" + i, pendingWindow);
                threads.add(thread);
                runThread(thread);
            }
            topicPartition2Threads.put(topicPartition, threads);
            selectedThread = threads.get(consumerRecordInfo.record().hashCode() % threads.size());
        }

        if(selectedThread != null){
            selectedThread.queue().add(consumerRecordInfo);
            log.debug("message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()) + "queued(" + selectedThread.queue().size() + " rest)");
        }

        return true;
    }

    private boolean checkHandlerTerminated(){
        for(List<OPMTMessageQueueHandlerThread> threads: topicPartition2Threads.values()){
            for(OPMTMessageQueueHandlerThread thread: threads){
                if(!thread.isTerminated()){
                    return false;
                }
            }
        }
        log.info("all handlers terminated");
        return true;
    }

    @Override
    public void consumerCloseNotify(Set<TopicPartition> topicPartitions){
        log.info("shutdown all handlers...");
        //停止所有handler
        for(List<OPMTMessageQueueHandlerThread> threads: topicPartition2Threads.values()){
            for(OPMTMessageQueueHandlerThread thread: threads){
                if(!thread.isTerminated()){
                    thread.close();
                }
            }
        }

        //提交最新处理消息的Offset
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            pendingWindow.commitLatest(false);
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
        for(List<OPMTMessageQueueHandlerThread> threads: topicPartition2Threads.values()){
            //不清除队列好像也可以
            for(OPMTMessageQueueHandlerThread thread: threads){
                if(!thread.isTerminated()){
                    thread.close();
                }
            }
        }

        //清楚topic分区与handler的映射
        topicPartition2Threads.clear();
        isRebalance.set(false);
    }

    private OPMTMessageQueueHandlerThread newThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets, String logHead, PendingWindow pendingWindow){
        return new OPMTMessageQueueHandlerThread(pendingOffsets, logHead, pendingWindow);
    }

    private void runThread(Runnable target){
        threads.submit(target);
    }

    private class OPMTMessageQueueHandlerThread extends AbstractMessageHandlersManager.MessageQueueHandlerThread {
        private PendingWindow pendingWindow;

        public OPMTMessageQueueHandlerThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets, String logHead, PendingWindow pendingWindow) {
            super(logHead, pendingOffsets);
            this.pendingWindow = pendingWindow;
        }

        @Override
        protected void commit(ConsumerRecordInfo record) {
            pendingWindow.commitFinished(record);
        }

    }
}
