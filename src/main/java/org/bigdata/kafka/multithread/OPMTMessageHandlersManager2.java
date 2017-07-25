package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 健勤 on 2017/7/18.
 * 基于OPOT思想的OPMT改进版本
 */
public class OPMTMessageHandlersManager2 extends AbstractMessageHandlersManager {
    private static Logger log = LoggerFactory.getLogger(OPMTMessageHandlersManager.class);
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new HashMap<>();
    private Map<TopicPartition, List<OPMTMessageQueueHandlerThread>> topicPartition2Threads = new HashMap<>();
    //所有消息处理线程在同一线程池维护
    private ThreadPoolExecutor threads = new ThreadPoolExecutor(2, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    private final int threadSizePerPartition;

    public OPMTMessageHandlersManager2() {
        this(Runtime.getRuntime().availableProcessors() * 2 - 1);
    }

    public OPMTMessageHandlersManager2(int threadSizePerPartition) {
        this.threadSizePerPartition = threadSizePerPartition;
    }

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets){
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
            //已有该topic分区对应的线程池启动
            //直接添加队列
            //round进队
            threads = topicPartition2Threads.get(topicPartition);
            selectedThread = threads.get(consumerRecordInfo.record().hashCode() % threads.size());
        }
        else{
            //没有该topic分区对应的线程池
            //先启动线程池,再添加至队列
            if(pendingWindow == null){
                log.info("new pending window");
                pendingWindow = new PendingWindow(1000, pendingOffsets);
                topicPartition2PendingWindow.put(topicPartition, pendingWindow);
            }
            threads = new ArrayList<>();
            log.info("init [" + threadSizePerPartition + "] message handler threads for topic-partition(" + topicPartition.topic() + "-" + topicPartition.partition() + ")");
            for(int i = 0; i < threadSizePerPartition; i++){
                OPMTMessageQueueHandlerThread thread = newThread(topicPartition.topic() + "-" + topicPartition.partition() + "#" + i, pendingOffsets, newMessageHandler(topicPartition.topic()), pendingWindow);
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
                    thread.stop();
                }
            }
        }

        //提交最新处理消息的Offset
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            pendingWindow.commitLatest(false);
        }
        topicPartition2PendingWindow.clear();


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
    }

    @Override
    public void consumerRebalanceNotify(){
        isRebalance.set(true);
        log.info("clean up handlers(not thread)");

        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            //提交已完成处理的消息的最大offset
            pendingWindow.commitLatest(false);
            pendingWindow.clean();
        }

        //关闭Handler执行,但不关闭线程,达到线程复用的效果
        for(List<OPMTMessageQueueHandlerThread> threads: topicPartition2Threads.values()){
            //不清除队列好像也可以
            for(OPMTMessageQueueHandlerThread thread: threads){
                if(!thread.isTerminated()){
                    thread.stop();
                }
            }
        }

        //清理消息处理线程Offset submit窗口
        topicPartition2PendingWindow.clear();

        //清楚topic分区与handler的映射
        topicPartition2Threads.clear();
        isRebalance.set(false);
    }

    private OPMTMessageQueueHandlerThread newThread(String LOG_HEAD, Map<TopicPartition, OffsetAndMetadata> pendingOffsets, MessageHandler messageHandler, PendingWindow pendingWindow){
        return new OPMTMessageQueueHandlerThread(LOG_HEAD, pendingOffsets, messageHandler, null, pendingWindow);
    }

    private void runThread(Runnable target){
        threads.submit(target);
    }

    private final class OPMTMessageQueueHandlerThread extends AbstractMessageHandlersManager.MessageQueueHandlerThread {
        private PendingWindow pendingWindow;

        public OPMTMessageQueueHandlerThread(String LOG_HEAD, Map<TopicPartition, OffsetAndMetadata> pendingOffsets, MessageHandler messageHandler, CommitStrategy commitStrategy, PendingWindow pendingWindow) {
            super(LOG_HEAD, pendingOffsets, messageHandler, commitStrategy);
            this.pendingWindow = pendingWindow;
        }

        @Override
        protected void commit(ConsumerRecordInfo record) {
            pendingWindow.commitFinished(record);
        }

    }
}
