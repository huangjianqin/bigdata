package org.kin.kafka.multithread.core;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kin.kafka.multithread.config.AppConfig;
import org.kin.kafka.multithread.configcenter.ReConfigable;
import org.kin.kafka.multithread.domain.ConsumerRecordInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Created by hjq on 2017/7/4.
 */
public class PendingWindow implements ReConfigable{
    private static final Logger log = LoggerFactory.getLogger("OPMT");
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets;
    private ConcurrentSkipListSet<ConsumerRecordInfo> queue;
    private int slidingWindow;

    //标识是否有处理线程正在判断窗口满足
    private AtomicBoolean isChecking = new AtomicBoolean(false);
    //记录上次commit offset
    //防止出现跳offset commit,也就是处理了提交了offset 1, 但是offset 345进入队列,而offset 2还没有完成处理时,直接提交offset 5的异常
    private long lastCommittedOffset = -1;
    //记录上一次处理pendingwindow的时间
    private long lastProcessTime = -1;

    public PendingWindow(int slidingWindow, Map<TopicPartition, OffsetAndMetadata> pendingOffsets) {
        log.info("init pendingWindow, slidingWindow size = " + slidingWindow);
        this.slidingWindow = slidingWindow;
        this.pendingOffsets = pendingOffsets;
        this.queue = new ConcurrentSkipListSet<>(new Comparator<ConsumerRecordInfo>(){

            @Override
            public int compare(ConsumerRecordInfo o1, ConsumerRecordInfo o2) {
                if(o2 == null){
                    return 1;
                }

                long offset1 = o1.record().offset();
                long offset2 = o2.record().offset();

                if(offset1 > offset2){
                    return 1;
                }
                else if(offset1 == offset2){
                    return 0;
                }
                else {
                    return -1;
                }
            }
        });
    }

    public void clean(){
        log.info("queue clean up");
        queue.clear();
    }

    /**
     * 当record为空时,也就是说明当前线程想抢占并提交有效连续的Offset
     * @param record
     */
    public void commitFinished(ConsumerRecordInfo record){
        if(record != null){
            lastProcessTime = System.currentTimeMillis();

            log.debug("consumer record " + record.record() + " finished");

            queue.add(record);

            //有点像是拿来主义,选举出leader进行处理(先到先得)
            //保证同一时间只有一条处理线程判断窗口满足
            //多线判断窗口满足有点难,需要加锁,感觉性能还不如控制一次只有一条线程判断窗口满足,无锁操作queue,其余处理线程直接进队
            //原子操作设置标识
            if(queue.size() >= slidingWindow){
                //队列大小满足窗口大小才去判断
                if(isChecking.compareAndSet(false, true)){
                    //判断是否满足窗口,若满足,则提交Offset
                    commitLatest(true);
                    isChecking.set(false);
                }
            }
        }
        else {
            long now = System.currentTimeMillis();
            //超过5s没有处理过PendingWindow,则进行处理
            if(now - lastProcessTime < 5000){
                return;
            }
            lastProcessTime = now;
            //尝试抢占并更新提交最长连续的offset
            if(isChecking.compareAndSet(false, true)){
                log.debug("PendingWindow doesn't be processed too long(>5s), so preempt to commit largest continued finished consumer records offsets");
                //判断是否满足窗口,若满足,则提交Offset
                commitLatest(false);
                isChecking.set(false);
            }
        }
    }

    public void commitLatest(boolean isInWindow){
        //队列为空,则返回
        //窗口大小都没满足,则返回
        if(queue.size() <= 0 || (isInWindow && queue.size() < slidingWindow)){
            return;
        }

        //如果队列头与上一次提交的offset不连续,直接返回
        long queueFirstOffset = queue.first().record().offset();
        if(lastCommittedOffset != -1 && queueFirstOffset - lastCommittedOffset != 1) {
            log.warn("last committed offset'" + lastCommittedOffset + "' is not continued with now queue head'" + queueFirstOffset + "'");
            return;
        }

        //复制视图
        ConsumerRecordInfo[] tmp =  new ConsumerRecordInfo[queue.size()];
        tmp = queue.toArray(tmp);//toArray方法是如果参数数组长度比queue少,会创建一个新的Array实例并返回

        //获取基本信息
        long maxOffset = tmp[0].record().offset();
        String topic = tmp[0].record().topic();
        int partition = tmp[0].record().partition();

        if(isInWindow){
            log.debug("try to commit largest continued finished consumer records offsets within certain window");
            //判断是否满足窗口
            long lastOffset = tmp[slidingWindow - 1].record().offset();
            //因为Offset是连续的,如果刚好满足窗口,则视图的第一个Offset和最后一个Offset相减更好等于窗口大小-1
            if(lastOffset - maxOffset == (slidingWindow - 1)){
                //满足窗口
                maxOffset = lastOffset;

                //因为Offset是连续的,尽管queue不断插入,但是永远不会排序插入到前slidingWindow个中,所以可以直接poll掉前slidingWindow个
                for(int i = 0; i < slidingWindow; i++){
                    if(queue.pollFirst().record().offset() != tmp[i].record().offset()){
                        throw new IllegalStateException("pendingwindow remove continue offset wrong, because poll offset is not same with the view head offset");
                    }
                }

                lastCommittedOffset = maxOffset;
            }
            else{
                //不满足,则直接返回
                return;
            }
        }
        else{
            log.debug("try to commit largest continued finished consumer records offsets");
            //需要提交处理完的连续的最新的Offset
            for(int i = 1; i < tmp.length; i++){
                long thisOffset = tmp[i].record().offset();
                //上一次成功连续并移除
                if(queue.pollFirst().record().offset() != tmp[i - 1].record().offset()){
                    throw new IllegalStateException("pendingwindow remove continue offset wrong, because poll offset is not same with the view head offset");
                }
                //判断offset是否连续
                if(thisOffset - maxOffset == 1){
                    //连续
                    maxOffset = thisOffset;
                }
                else{
                    //非连续
                    //如果是获取queue最大Offset,就直接退出循环
                    break;
                }
            }
            //找到连续Offset(length > 1),要移除maxoffset
            if(maxOffset != tmp[0].record().offset()){
                if(queue.pollFirst().record().offset() != maxOffset){
                    throw new IllegalStateException("pendingwindow remove continue offset wrong, because poll offset is not same with the view head offset");
                }
            }

            lastCommittedOffset = maxOffset;
        }

        pendingToCommit(new TopicPartition(topic, partition), new OffsetAndMetadata(maxOffset + 1));
    }

    private void pendingToCommit(TopicPartition topicPartition, OffsetAndMetadata offset){
        log.debug("pending to commit offset = " + offset);
        this.pendingOffsets.put(topicPartition, offset);
    }

    @Override
    public void reConfig(Properties newConfig) {
        log.info("pending window reconfiging...");

        //message handler manager判断有改变,程序才会执行到这里
        this.slidingWindow = Integer.valueOf(newConfig.getProperty(AppConfig.PENDINGWINDOW_SLIDINGWINDOW));

        log.info("pending window reconfiged");
    }
}
