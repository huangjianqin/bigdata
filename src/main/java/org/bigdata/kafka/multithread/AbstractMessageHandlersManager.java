package org.bigdata.kafka.multithread;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected Map<TopicPartition, CommitStrategy> topic2CommitStrategy = new ConcurrentHashMap<>();
    protected Map<String, MessageHandler> topic2Handler = new ConcurrentHashMap<>();
    protected AtomicBoolean isRebalance = new AtomicBoolean(false);

    public void registerHandler(String topic, MessageHandler handler){
        try {
            if (topic2Handler.containsKey(topic)){
                topic2Handler.get(topic).cleanup();
            }
            handler.setup();
            topic2Handler.put(topic, handler);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void registerHandlers(Map<String, MessageHandler> topic2Handler){
        if(topic2Handler == null){
            return;
        }
        for(Map.Entry<String, MessageHandler> entry: topic2Handler.entrySet()){
            registerHandler(entry.getKey(), entry.getValue());
        }
    }

    public void registerCommitStrategy(TopicPartition topicPartition, CommitStrategy strategy){
        try {
            if (topic2CommitStrategy.containsKey(topicPartition)){
                topic2CommitStrategy.get(topicPartition).cleanup();
            }
            strategy.setup();
            topic2CommitStrategy.put(topicPartition, strategy);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void registerCommitStrategies(Map<TopicPartition, CommitStrategy> topic2CommitStrategy){
        if(topic2CommitStrategy == null){
            return;
        }
        for(Map.Entry<TopicPartition, CommitStrategy> entry: topic2CommitStrategy.entrySet()){
            registerCommitStrategy(entry.getKey(), entry.getValue());
        }
    }

    protected void cleanMsgHandlersAndCommitStrategies(){
        log.info("cleaning message handlers & commit stratgies...");
        try {
            //清理handler和commitstrategy
            for(MessageHandler messageHandler: topic2Handler.values()){
                messageHandler.cleanup();
            }

            for(CommitStrategy commitStrategy: topic2CommitStrategy.values()){
                commitStrategy.cleanup();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("message handlers & commit stratgies cleaned");
    }

    protected abstract class MessageQueueHandlerThread implements Runnable{
        protected Logger log = LoggerFactory.getLogger(MessageQueueHandlerThread.class);
        private String LOG_HEAD = "";

        //等待需要提交的Offset队列
        protected Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets;
        //按消息插入顺序排序
        protected LinkedBlockingQueue<ConsumerRecordInfo> queue = new LinkedBlockingQueue<>();

        protected boolean isStooped = false;
        protected boolean isTerminated = false;

        //最近一次处理的最大的offset
        protected ConsumerRecord lastRecord = null;

        public MessageQueueHandlerThread(String LOG_HEAD, Map<TopicPartitionWithTime, OffsetAndMetadata> pendingOffsets) {
            this.LOG_HEAD = LOG_HEAD;
            this.pendingOffsets = pendingOffsets;
        }

        public Queue<ConsumerRecordInfo> queue() {
            return queue;
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
            TopicPartition topicPartition = record.topicPartition();
            MessageHandler messageHandler = topic2Handler.get(topicPartition.topic());
            if(messageHandler == null){
                log.info(LOG_HEAD + " message handler not set, use default");
                //采用默认的Handler
                messageHandler = new DefaultMessageHandler();
                topic2Handler.put(topicPartition.topic(), messageHandler);
            }
            messageHandler.handle(record.record());
        }

        protected void commit(ConsumerRecordInfo record){
            CommitStrategy commitStrategy = topic2CommitStrategy.get(new TopicPartition(lastRecord.topic(), lastRecord.partition()));
            if(commitStrategy == null){
                //采用默认的commitStrategy
                commitStrategy = new DefaultCommitStrategy();
                log.info(LOG_HEAD + " commit strategy not set, use default");
                topic2CommitStrategy.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()), commitStrategy);
            }

            if(commitStrategy.isToCommit(record.record())){
                log.info(LOG_HEAD + " satisfy commit strategy, pending to commit");
                pendingOffsets.put(new TopicPartitionWithTime(new TopicPartition(lastRecord.topic(), lastRecord.partition()), System.currentTimeMillis()), new OffsetAndMetadata(lastRecord.offset() + 1));
                lastRecord = null;
            }
        }

        protected void close(){
            log.info(LOG_HEAD + " stopping...");
            this.isStooped = true;
        }

        protected void preTerminated(){
            terminate();
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
