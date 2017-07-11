package org.bigdata.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hjq on 2017/6/19.
 * OPOT ==> one partition one Thread
 */
public class OPOTMessageHandlersManager extends AbstractMessageHandlersManager{
    private static Logger log = LoggerFactory.getLogger(OPOTMessageHandlersManager.class);
    private static OPOTMessageHandlersManager handlersManager;
    private Map<TopicPartition, MessageHandlerThread> topicPartition2Thread = new ConcurrentHashMap<>();
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
            MessageHandlerThread thread = newThread(pendingOffsets, topicPartition.topic() + "-" + topicPartition.partition());
            topicPartition2Thread.put(topicPartition, thread);
            thread.queue.add(consumerRecordInfo);
            runThread(thread);
            log.debug("message: " + StrUtil.consumerRecordDetail(consumerRecordInfo.record()) + "queued(" + thread.queue.size() + " rest)");
        }

        return true;
    }

    private boolean checkHandlerTerminated(){
        for(MessageHandlerThread thread: topicPartition2Thread.values()){
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
        for(MessageHandlerThread thread: topicPartition2Thread.values()){
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
        for(MessageHandlerThread thread: topicPartition2Thread.values()){
            //不清除队列好像也可以
            thread.close();
        }

        //清楚topic分区与handler的映射
        topicPartition2Thread.clear();
        isRebalance.set(false);
    }

    private MessageHandlerThread newThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets, String logHead){
        return new MessageHandlerThread(pendingOffsets, logHead);
    }

    private void runThread(Runnable target){
        threads.submit(target);
    }

    private class MessageHandlerThread implements Runnable{
        private Logger log = LoggerFactory.getLogger(MessageHandlerThread.class);
        private String LOG_HEAD = "";
        private Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets;
        //按消息插入顺序排序
        private LinkedBlockingQueue<ConsumerRecordInfo> queue = new LinkedBlockingQueue<>();
        private boolean isStooped = false;
        private boolean isTerminated = false;
        //最近一次处理的最大的offset
        private ConsumerRecord lastRecord = null;

        public MessageHandlerThread(Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets, String logHead) {
            this.pendingOffsets = pendingOffsets;
            this.LOG_HEAD =logHead;
        }

        public Queue<ConsumerRecordInfo> queue() {
            return queue;
        }

        public boolean isTerminated() {
            return isTerminated;
        }

        public void close(){
            log.info(LOG_HEAD + " stopping...");
            this.isStooped = true;
        }

        @Override
        public void run() {
            log.info(LOG_HEAD + " start up");
            while(!this.isStooped && !Thread.currentThread().isInterrupted()){
                try {
                    ConsumerRecordInfo record = queue.poll(100, TimeUnit.MILLISECONDS);
                    //队列中有消息需要处理
                    if(record != null){
                        execute(record);
                        //保存最新的offset
                        if(lastRecord != null){
                            if(record.record().offset() > lastRecord.offset()){
                                lastRecord = record.record();
                            }
                        }
                        else{
                            lastRecord = record.record();
                        }

                        CommitStrategy commitStrategy = OPOTMessageHandlersManager.this.topic2CommitStrategy.get(new TopicPartition(lastRecord.topic(), lastRecord.partition()));
                        if(commitStrategy == null){
                            //采用默认的commitStrategy
                            commitStrategy = new DefaultCommitStrategy();
                            log.info(LOG_HEAD + " commit strategy not set, use default");
                            OPOTMessageHandlersManager.this.topic2CommitStrategy.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()), commitStrategy);
                        }

                        if(commitStrategy.isToCommit(record.record())){
                            log.info(LOG_HEAD + " satisfy commit strategy, pending to commit");
                            pendingOffsets.put(new TopicPartitionWithTime(new TopicPartition(lastRecord.topic(), lastRecord.partition()), System.currentTimeMillis()), new OffsetAndMetadata(lastRecord.offset() + 1));
                            lastRecord = null;
                        }
                    }
                    else{
                        //队列poll超时
//                        log.info(LOG_HEAD + " thread idle --> sleep 200ms");
                        Thread.sleep(200);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            //只有两种情况:
            //1:rebalance 抛弃这些待处理信息
            //2:关闭consumer 抛弃这些待处理信息,提交最近处理的offset

            if(!isRebalance.get()){
                log.info(LOG_HEAD + " closing consumer should commit last offsets sync now");
                if(lastRecord != null){
                    pendingOffsets.put(new TopicPartitionWithTime(new TopicPartition(lastRecord.topic(), lastRecord.partition()),
                            System.currentTimeMillis()), new OffsetAndMetadata(lastRecord.offset() + 1));
                }
            }

            isTerminated = true;
            log.info(LOG_HEAD + " terminated");
        }

        private void execute(ConsumerRecordInfo record){
            try {
                doExecute(record);
                record.callBack(null);
            } catch (Exception e) {
                try {
                    record.callBack(e);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }

        private void doExecute(ConsumerRecordInfo record) throws Exception {
            TopicPartition topicPartition = record.topicPartition();
            MessageHandler messageHandler = OPOTMessageHandlersManager.this.topic2Handler.get(topicPartition.topic());
            if(messageHandler == null){
                log.info(LOG_HEAD + " message handler not set, use default");
                //采用默认的Handler
                messageHandler = new DefaultMessageHandler();
                OPOTMessageHandlersManager.this.topic2Handler.put(topicPartition.topic(), messageHandler);
            }
            messageHandler.handle(record.record());
        }
    }
}
