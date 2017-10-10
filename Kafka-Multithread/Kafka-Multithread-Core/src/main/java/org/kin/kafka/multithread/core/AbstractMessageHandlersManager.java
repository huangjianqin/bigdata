package org.kin.kafka.multithread.core;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.api.MessageHandler;
import org.kin.kafka.multithread.api.CommitStrategy;
import org.kin.kafka.multithread.configcenter.ReConfigable;
import org.kin.kafka.multithread.utils.AppConfigUtils;
import org.kin.kafka.multithread.common.ConsumerRecordInfo;
import org.kin.kafka.multithread.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by hjq on 2017/7/4.
 */
public abstract class AbstractMessageHandlersManager implements MessageHandlersManager {
    private static final Logger log = LoggerFactory.getLogger(AbstractMessageHandlersManager.class);
    protected AtomicBoolean isRebalance = new AtomicBoolean(false);
    protected AtomicBoolean isReconfig = new AtomicBoolean(false);
    protected Properties config;

    private Map<String, Class<? extends MessageHandler>> topic2HandlerClass;
    private Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass;

    AbstractMessageHandlersManager(Properties config){
        this.config = config;

        topic2HandlerClass = new HashMap<>();
        topic2CommitStrategyClass = new HashMap<>();

        for(String topic: AppConfigUtils.getSubscribeTopic(config)){
            topic2HandlerClass.put(topic, AppConfigUtils.getMessageHandlerClass(config));
            topic2CommitStrategyClass.put(topic, AppConfigUtils.getCommitStrategyClass(config));
        }
    }

    /**
     * 注册topics对应的message handlers class实例
     * @param topic2HandlerClass
     */
    public void registerHandlers(Map<String, Class<? extends MessageHandler>> topic2HandlerClass){
        this.topic2HandlerClass = topic2HandlerClass;
    }

    /**
     * 注册topics对应的commit Strategies class实例
     * @param topic2CommitStrategyClass
     */
    public void registerCommitStrategies(Map<String, Class<? extends CommitStrategy>> topic2CommitStrategyClass){
        this.topic2CommitStrategyClass = topic2CommitStrategyClass;
    }

    /**
     * 通过class信息实例化Message handler并调用setup方法进行初始化
     * @param topic
     * @return
     */
    protected MessageHandler newMessageHandler(String topic){
        Class<? extends MessageHandler> claxx = topic2HandlerClass.get(topic);
        if(claxx != null){
            try {
                MessageHandler messageHandler = ClassUtils.instance(claxx);
                //初始化message handler
                messageHandler.setup(config);

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

    /**
     * 通过class信息实例化Commit strategy并调用setup方法进行初始化
     * @param topic
     * @return
     */
    protected CommitStrategy newCommitStrategy(String topic){
        Class<? extends CommitStrategy> claxx = topic2CommitStrategyClass.get(topic);
        if(claxx != null){
            try {
                CommitStrategy commitStrategy = ClassUtils.instance(claxx);
                //初始化message handler
                commitStrategy.setup(config);

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

    protected boolean checkHandlerTerminated(Collection<MessageQueueHandlerThread> messageQueueHandlerThreads){
        for(MessageQueueHandlerThread thread: messageQueueHandlerThreads){
            if(!thread.isTerminated()){
                return false;
            }
        }
        log.info("all target handlers terminated");
        return true;
    }

    /**
     * 等待线程池中线程空闲,即可关闭线程或进行其他操作
     * 如果超时返回true,否则返回false
     */
    protected boolean waitingThreadPoolIdle(Collection<MessageQueueHandlerThread> messageQueueHandlerThreads, long timeout){
        if(messageQueueHandlerThreads.size() < 0){
            return false;
        }
        if(timeout < 0){
            throw new IllegalStateException("timeout should be greater than 0(now is " + timeout + ")");
        }
        long baseTime = System.currentTimeMillis();
        while(!checkHandlerTerminated(messageQueueHandlerThreads)){
            if(System.currentTimeMillis() - baseTime > timeout){
                log.warn("target message handlers terminate time out!!!");
                return true;
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName());
                log.error("now operation interrupted by thread '" + Thread.currentThread().getName() + "'");
            }
        }

        return false;
    }

    /**
     * 更新更新配置标识状态
     * @param expected
     * @param update
     * @return
     */
    public boolean updateReConfigStatus(boolean expected, boolean update){
        return isReconfig.compareAndSet(expected, update);
    }

    /**
     * 内部抽象的消息处理线程的默认实现
     */
    protected abstract class MessageQueueHandlerThread implements Runnable, ReConfigable{
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

        /**
         * 判断线程是否终止
         * @return
         */
        protected boolean isTerminated() {
            return isTerminated;
        }

        /**
         * 终止线程
         */
        private void terminate(){
            isTerminated = true;
            log.info(LOG_HEAD + " terminated");
        }

        /**
         * 线程启动后动作
         */
        protected void afterStart(){
            log.info(LOG_HEAD + " start up");
        }

        /**
         * 执行消息处理,包括调用callback
         * @param record
         */
        private void execute(ConsumerRecordInfo record){
            try {
                doExecute(record);
                record.callBack(messageHandler, commitStrategy, null);
            } catch (Exception e) {
                try {
                    record.callBack(messageHandler, commitStrategy, e);
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }

        /**
         * 真正对消息进行处理
         * @param record
         * @throws Exception
         */
        protected void doExecute(ConsumerRecordInfo record) throws Exception {
            messageHandler.handle(record.record());
        }

        /**
         * 消息处理完成,提交Offset(判断成功才提交)
         * @param record
         */
        protected void commit(ConsumerRecordInfo record){
            if(commitStrategy.isToCommit(messageHandler, record.record())){
                log.debug(LOG_HEAD + " satisfy commit strategy, pending to commit");
                pendingOffsets.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()), new OffsetAndMetadata(lastRecord.offset() + 1));
                lastRecord = null;
            }
        }

        /**
         * 提交最新的Offset,用于Rebalance
         */
        protected void commitLatest(){
            if(lastRecord != null){
                log.debug(LOG_HEAD + " commit lastest Offset");
                pendingOffsets.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()), new OffsetAndMetadata(lastRecord.offset() + 1));
                lastRecord = null;
            }
        }

        /**
         * 定制消息处理
         */
        protected void stop(){
            log.info(LOG_HEAD + " stopping...");
            this.isStooped = true;
        }

        /**
         * 线程终止前的动作
         */
        protected void preTerminated(){
            //释放message handler和CommitStrategy的资源
            try {
                if(commitStrategy != null){
                    //OPMT模式下没有CommitStrategy,所以此处需要判断
                    commitStrategy.cleanup();
                }
                if(messageHandler != null){
                    messageHandler.cleanup();
                }
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
                    ConsumerRecordInfo record = queue.poll();
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

                    /**
                     * 配置更新中,停止处理消息
                     */
                    while(isReconfig.get()){
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException e) {
                    log.info(Thread.currentThread().getName() + " interrupted");
                }
            }
            //线程结束前的资源释放或其他操作
            preTerminated();
        }
    }

    public static enum MsgHandlerManagerModel{
        OPOT("OPOT"), OPMT("OPMT"), OPMT2("OPMT2"), OCOT("OCOT");

        private String desc;

        MsgHandlerManagerModel(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }

        public static MsgHandlerManagerModel getByDesc(String arg){
            for(MsgHandlerManagerModel model: values()){
                if(arg.toUpperCase().equals(model.getDesc())){
                    return model;
                }
            }

            throw new IllegalStateException(arg + " => unknown message handler manager model");
        }
    }
}
