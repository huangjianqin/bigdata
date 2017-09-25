package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.MessageHandler;
import org.kin.kafka.multithread.api.CommitStrategy;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.kin.kafka.multithread.utils.ConsumerRecordInfo;
import org.kin.kafka.multithread.utils.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by 健勤 on 2017/7/18.
 * 基于OPOT和滑动窗口思想的OPMT改进版本
 *
 * 每个分区多个线程处理,每个线程拥有一个队列,dispatch分发消息,消息负载均衡地在对应分区的所有处理线程之一插队
 *
 * 相比OPMT,更可观地控制commit strategy以及消息处理过程
 *
 * 可以认为是OPOT的多线程版本,但是也可以实现OPOT模型(只要对应分区的处理线程设置为1)
 *
 * 性能提高:
 * 1.改进PendingWindow类
 * 2.改进负载均衡策略
 */
public class OPMT2MessageHandlersManager extends AbstractMessageHandlersManager {
    private static final Logger log = LoggerFactory.getLogger(OPMTMessageHandlersManager.class);
    private Map<TopicPartition, PendingWindow> topicPartition2PendingWindow = new HashMap<>();
    private Map<TopicPartition, List<OPMT2MessageQueueHandlerThread>> topicPartition2Threads = new HashMap<>();
    //所有消息处理线程在同一线程池维护
    private final ThreadPoolExecutor threads = new ThreadPoolExecutor(2, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
    private int threadSizePerPartition;

    public OPMT2MessageHandlersManager() {
        super(AppConfig.DEFAULT_APPCONFIG);
    }

    public OPMT2MessageHandlersManager(Properties config) {
        super(config);
        this.threadSizePerPartition = Integer.valueOf(config.get(AppConfig.OPMT2_THREADSIZEPERPARTITION).toString());
    }

    @Override
    public boolean dispatch(ConsumerRecordInfo consumerRecordInfo, Map<TopicPartition, OffsetAndMetadata> pendingOffsets){
        log.debug("dispatching message: " + StrUtils.consumerRecordDetail(consumerRecordInfo.record()));

        if(isRebalance.get()){
            log.debug("dispatch failure ~~~ rebalancing...");
            return false;
        }

        TopicPartition topicPartition = consumerRecordInfo.topicPartition();
        List<OPMT2MessageQueueHandlerThread> threads = null;
        OPMT2MessageQueueHandlerThread selectedThread = null;
        PendingWindow pendingWindow = topicPartition2PendingWindow.get(topicPartition);
        if(topicPartition2Threads.containsKey(topicPartition)){
            //已有该topic分区对应的线程池启动
            //直接添加队列
            //round进队
            threads = topicPartition2Threads.get(topicPartition);
            if(threads.size() < threadSizePerPartition){
                //如果配置的线程数增加,dispatch时动态增加处理线程数
                log.info("add [" + (threadSizePerPartition - threads.size()) + "] message handler threads for topic-partition(" + topicPartition.topic() + "-" + topicPartition.partition() + ")");
                for(int i = threads.size(); i < threadSizePerPartition; i++){
                    OPMT2MessageQueueHandlerThread thread = newThread(topicPartition.topic() + "-" + topicPartition.partition() + "#" + i, pendingOffsets, newMessageHandler(topicPartition.topic()), pendingWindow);
                    threads.add(thread);
                    runThread(thread);
                }
            }
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
                OPMT2MessageQueueHandlerThread thread = newThread(topicPartition.topic() + "-" + topicPartition.partition() + "#" + i, pendingOffsets, newMessageHandler(topicPartition.topic()), pendingWindow);
                threads.add(thread);
                runThread(thread);
            }
            topicPartition2Threads.put(topicPartition, threads);
            selectedThread = threads.get(consumerRecordInfo.record().hashCode() % threads.size());
        }

        if(selectedThread != null){
            selectedThread.queue().add(consumerRecordInfo);
            log.debug("message: " + StrUtils.consumerRecordDetail(consumerRecordInfo.record()) + "queued(" + selectedThread.queue().size() + " rest)");
        }

        return true;
    }

    private boolean checkHandlerTerminated(){
        for(List<OPMT2MessageQueueHandlerThread> threads: topicPartition2Threads.values()){
            for(OPMT2MessageQueueHandlerThread thread: threads){
                if(!thread.isTerminated()){
                    return false;
                }
            }
        }
        log.info("all handlers terminated");
        return true;
    }

    @Override
    public void consumerCloseNotify(){
        log.info("shutdown all handlers...");
        List<OPMT2MessageQueueHandlerThread> allThreads = new ArrayList<>();
        //停止所有handler
        for(List<OPMT2MessageQueueHandlerThread> threads: topicPartition2Threads.values()){
            for(OPMT2MessageQueueHandlerThread thread: threads){
                if(!thread.isTerminated()){
                    thread.stop();
                    allThreads.add(thread);
                }
            }
        }

        //提交最新处理消息的Offset
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            pendingWindow.commitLatest(false);
        }
        topicPartition2PendingWindow.clear();


        //等待所有handler完成,超过10s,强制关闭
        boolean isTimeout = waitingThreadPoolIdle((Collection)allThreads, 10000);

        //关闭线程池
        if(!isTimeout){
            log.info("shutdown thread pool...");
            threads.shutdown();
        }
        else{
            log.info("force shutdown thread pool...");
            threads.shutdownNow();
        }
        log.info("thread pool terminated");
    }
    /**
     * 之前分配到的TopicPartitions
     * @param topicPartitions 当前分配到的分区
     */
    @Override
    public void consumerRebalanceNotify(Set<TopicPartition> topicPartitions){
        isRebalance.set(true);
        log.info("clean up handlers(not thread)");

        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            //提交已完成处理的消息的最大offset
            pendingWindow.commitLatest(false);
        }
    }

    /**
     * @param topicPartitions 之前分配到但此次又没有分配到的TopicPartitions
     */
    @Override
    public void doOnConsumerReAssigned(Set<TopicPartition> topicPartitions) {
        List<OPMT2MessageQueueHandlerThread> allThreads = new ArrayList<>();
        List<PendingWindow> allPendingWindow = new ArrayList<>();
        //关闭Handler执行,但不关闭线程,达到线程复用的效果
        for(TopicPartition topicPartition: topicPartitions){
            //不清除队列好像也可以
            for(OPMT2MessageQueueHandlerThread thread: topicPartition2Threads.get(topicPartition)){
                if(!thread.isTerminated()){
                    thread.stop();
                }
            }
            //移除滑动窗口
            allPendingWindow.add(topicPartition2PendingWindow.remove(topicPartition));
            //移除属于该分区的线程
            allThreads.addAll(topicPartition2Threads.remove(topicPartition));
        }

        //等待线程池中线程空闲,如果超过3s,则抛异常,并释放资源
        boolean isTimeOut = waitingThreadPoolIdle((Collection)allThreads, 3000);
        if(isTimeOut){
            log.warn("waiting for target message handlers terminated timeout when rebalancing!!!");
            System.exit(-1);
        }

        isRebalance.set(false);
    }

    private OPMT2MessageQueueHandlerThread newThread(String LOG_HEAD, Map<TopicPartition, OffsetAndMetadata> pendingOffsets, MessageHandler messageHandler, PendingWindow pendingWindow){
        return new OPMT2MessageQueueHandlerThread(LOG_HEAD, pendingOffsets, messageHandler, null, pendingWindow);
    }

    private void runThread(Runnable target){
        threads.submit(target);
    }

    @Override
    public void reConfig(Properties newConfig) {
        log.info("OPMT2 message handler manager reconfiging...");
        int threadSizePerPartition = this.threadSizePerPartition;

        if(AppConfigUtils.isConfigItemChange(threadSizePerPartition, newConfig, AppConfig.OPMT2_THREADSIZEPERPARTITION)){
            threadSizePerPartition = Integer.valueOf(newConfig.getProperty(AppConfig.OPMT2_THREADSIZEPERPARTITION));
            if(threadSizePerPartition > 0){
                //仅仅是处理资源减少的情况,资源动态增加在dispatch中处理
                if(threadSizePerPartition < this.threadSizePerPartition){
                    log.info("reduce message handler threads per partition(size = " + (this.threadSizePerPartition - threadSizePerPartition) + ")");
                    for(TopicPartition key: topicPartition2Threads.keySet()){
                        List<OPMT2MessageQueueHandlerThread> threads = topicPartition2Threads.get(key);

                        //被移除处理线程所拥有的待处理消息
                        List<ConsumerRecordInfo> unhandleConsumerRecordInfos = new ArrayList<>();
                        for(int i = 0; i < this.threadSizePerPartition - threadSizePerPartition; i++){
                            OPMT2MessageQueueHandlerThread thread = threads.remove(i);
                            thread.stop();
                            //缓存待处理消息
                            unhandleConsumerRecordInfos.addAll(thread.queue);
                        }

                        //平均分配给'存活'的处理线程
                        for(int i = 0; i < unhandleConsumerRecordInfos.size(); i++){
                            threads.get(i % threads.size()).queue.add(unhandleConsumerRecordInfos.get(i));
                        }
                    }
                }
                this.threadSizePerPartition = threadSizePerPartition;
            }
            else {
                throw new IllegalStateException("config args 'threadSizePerPartition' state wrong");
            }
        }

        //更新pendingwindow的配置
        for(PendingWindow pendingWindow: topicPartition2PendingWindow.values()){
            pendingWindow.reConfig(newConfig);
        }

        //更新每一条处理线程的配置
        for(List<OPMT2MessageQueueHandlerThread> threads: topicPartition2Threads.values()){
            for(OPMT2MessageQueueHandlerThread thread: threads){
                thread.reConfig(newConfig);
            }
        }
        log.info("OPMT2 message handler manager reconfiged");
    }

    private final class OPMT2MessageQueueHandlerThread extends AbstractMessageHandlersManager.MessageQueueHandlerThread {
        private PendingWindow pendingWindow;

        public OPMT2MessageQueueHandlerThread(String LOG_HEAD, Map<TopicPartition, OffsetAndMetadata> pendingOffsets, MessageHandler messageHandler, CommitStrategy commitStrategy, PendingWindow pendingWindow) {
            super(LOG_HEAD, pendingOffsets, messageHandler, commitStrategy);
            this.pendingWindow = pendingWindow;
        }

        @Override
        protected void commit(ConsumerRecordInfo record) {
            pendingWindow.commitFinished(record);
        }

        @Override
        public void reConfig(Properties newConfig) {
            //不需要实现
            log.info("OPMT2 message handler thread reconfiging...");
            log.info("OPMT2 message handler thread reconfiged");
        }
    }
}
