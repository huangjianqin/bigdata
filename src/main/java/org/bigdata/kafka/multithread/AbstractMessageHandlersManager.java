package org.bigdata.kafka.multithread;


import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hjq on 2017/7/4.
 */
public abstract class AbstractMessageHandlersManager implements MessageHandlersManager {
    private static Logger log = LoggerFactory.getLogger(AbstractMessageHandlersManager.class);
    protected AtomicBoolean isRebalance = new AtomicBoolean(false);

    private Map<String, Class<? extends MessageHandler>> topic2HandlerClass = new HashMap<>();
    private Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass = new HashedMap();

    public void registerHandlers(Map<String, Class<? extends MessageHandler>> topic2HandlerClass){
        this.topic2HandlerClass = topic2HandlerClass;
    }

    public void registerCommitStrategies(Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass){
        this.topic2CommitStrategyClass = topic2CommitStrategyClass;
    }

    protected MessageHandler newMessageHandler(String topic){
        Class<? extends MessageHandler> claxx = topic2HandlerClass.get(topic);
        if(claxx != null){
            try {
                MessageHandler messageHandler = ClassUtil.instance(claxx);
                //初始化message handler
                messageHandler.setup();

                return messageHandler;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalStateException("appliction must set a message handler for one topic");
    }

    protected CommitStrategy newCommitStrategy(String topic){
        Class<? extends CommitStrategy> claxx = topic2CommitStrategyClass.get(topic);
        if(claxx != null){
            try {
                CommitStrategy commitStrategy = ClassUtil.instance(claxx);
                //初始化message handler
                commitStrategy.setup();

                return commitStrategy;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalStateException("appliction must set a commit strategy for one topic");
    }

    protected abstract class MessageQueueHandlerThread implements Runnable{
        protected Logger log = LoggerFactory.getLogger(MessageQueueHandlerThread.class);
        private String LOG_HEAD = "";

        //等待需要提交的Offset队列
        protected Map<TopicPartition, OffsetAndMetadata> pendingOffsets;
        //按消息插入顺序排序
        protected LinkedBlockingQueue<ConsumerRecordInfo> queue = new LinkedBlockingQueue<>();

        //绑定的消息处理器
        MessageHandler messageHandler;
        //绑定的Offset提交策略
        CommitStrategy commitStrategy;

        protected boolean isStooped = false;
        protected boolean isTerminated = false;

        //最近一次处理的最大的offset
        protected ConsumerRecord lastRecord = null;

        public MessageQueueHandlerThread(String LOG_HEAD, Map<TopicPartition, OffsetAndMetadata> pendingOffsets, MessageHandler messageHandler, CommitStrategy commitStrategy) {
            this.LOG_HEAD = LOG_HEAD;
            this.pendingOffsets = pendingOffsets;
            this.messageHandler = messageHandler;
            this.commitStrategy = commitStrategy;
        }

        public Queue<ConsumerRecordInfo> queue() {
            return queue;
        }

        public MessageHandler messageHandler(){
            return messageHandler;
        }

        public CommitStrategy commitStrategy(){
            return commitStrategy;
        }

        public String LOG_HEAD(){
            return LOG_HEAD;
        }

        protected boolean isTerminated() {
            return isTerminated;
        }

        private void terminate(){
            isTerminated = true;
            log.info(LOG_HEAD + " terminated");
        }

        protected void afterStart(){
            log.info(LOG_HEAD + " start up");
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

        protected void doExecute(ConsumerRecordInfo record) throws Exception {
            messageHandler.handle(record.record());
        }

        protected void commit(ConsumerRecordInfo record){
            if(commitStrategy.isToCommit(record.record())){
                log.debug(LOG_HEAD + " satisfy commit strategy, pending to commit");
                pendingOffsets.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()), new OffsetAndMetadata(lastRecord.offset() + 1));
                lastRecord = null;
            }
        }

        protected void close(){
            log.info(LOG_HEAD + " stopping...");
            this.isStooped = true;
        }

        protected void preTerminated(){
            //释放message handler和CommitStrategy的资源
            try {
                commitStrategy.cleanup();
                messageHandler.cleanup();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                //无论如何都要释放资源
                terminate();
            }
        }

        @Override
        public void run() {
            //线程开始前的初始化或准备工作
            afterStart();

            while(!this.isStooped && !Thread.currentThread().isInterrupted()){
                try {
                    ConsumerRecordInfo record = queue.poll(100, TimeUnit.MILLISECONDS);
                    //队列中有消息需要处理
                    if(record != null){
                        //对Kafka消息的处理
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

                        //提交Offset
                        //并不是真正让consumer提交Offset,视具体实现而定
                        commit(record);
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
            //线程结束前的资源释放或其他操作
            preTerminated();
        }
    }
}
