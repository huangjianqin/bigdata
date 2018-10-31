package org.kin.framework.concurrent;

import com.google.common.base.Preconditions;
import org.kin.framework.concurrent.impl.HashPartitioner;
import org.kin.framework.utils.ExceptionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2017/10/26.
 * 利用Task的某种属性将task分区,从而达到统一类的task按submit/execute顺序在同一线程执行
 */
public class PartitionTaskExecutor<K> {
    //分区数
    private volatile int partitionNum;

    //分区算法
    private Partitioner<K> partitioner;
    //执行线程池
    private ThreadPoolExecutor threadPool;
    //所有分区执行线程实例
    //lazy init
    private volatile PartitionTask[] partitionTasks;

    public PartitionTaskExecutor() {
        this(10);
    }

    public PartitionTaskExecutor(int partitionNum) {
        this(partitionNum, new HashPartitioner<>());
    }

    public PartitionTaskExecutor(int partitionNum, Partitioner<K> partitioner) {
        this.partitionNum = partitionNum;
        this.threadPool = new ThreadPoolExecutor(partitionNum, Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.threadPool.allowCoreThreadTimeOut(true);
        this.partitioner = partitioner;
        this.partitionTasks = new PartitionTaskExecutor.PartitionTask[this.partitionNum];
    }

    /**
     *
     * @param threadPool 如果最小线程数 < @param partitionNum, 则真实的@param partitionNum=@param threadPool的最大线程数
     */
    public PartitionTaskExecutor(int partitionNum, Partitioner<K> partitioner, ThreadPoolExecutor threadPool) {
        this.partitionNum = partitionNum;
        this.partitioner = partitioner;
        this.threadPool = threadPool;
        this.partitionTasks = new PartitionTaskExecutor.PartitionTask[this.partitionNum];
    }

    private PartitionTask getOrCreatePartitionTask(int partition){
        if (0 < partition && partition < partitionTasks.length) {
            PartitionTask partitionTask = partitionTasks[partition];
            if (partitionTask == null) {
                partitionTask = new PartitionTask();
                partitionTasks[partition] = partitionTask;
                threadPool.execute(partitionTask);
            }

            return partitionTask;
        } else {
            return null;
        }
    }

    public Future<?> execute(K key, Runnable task){
        PartitionTask partitionTask = getOrCreatePartitionTask(partitioner.toPartition(key, partitionNum));
        if(partitionTask != null){
            FutureTask futureTask = new FutureTask(task, null);

            partitionTask.execute(new Task(key, futureTask));

            return futureTask;
        }
        else{
            return null;
        }
    }

    public <T> Future<T> execute(K key, Runnable task, T value){
        PartitionTask partitionTask = getOrCreatePartitionTask(partitioner.toPartition(key, partitionNum));
        if(partitionTask != null){
            FutureTask futureTask = new FutureTask(task, value);

            partitionTask.execute(new Task(key, futureTask));

            return futureTask;
        }
        else{
            return null;
        }
    }

    public <T> Future<T> execute(K key, Callable<T> task){
        PartitionTask partitionTask = getOrCreatePartitionTask(partitioner.toPartition(key, partitionNum));
        if(partitionTask != null){
            FutureTask<T> futureTask = new FutureTask(task);

            partitionTask.execute(new Task(key, futureTask));

            return futureTask;
        }
        else{
            return null;
        }
    }

    public void shutdown(){
        //先关闭执行线程实例再关闭线程池
        //关闭并移除分区执行线程实例,且缓存
        for(PartitionTask task: partitionTasks){
            task.close();
        }
        threadPool.shutdown();
        //help gc
        partitionTasks = null;
        threadPool = null;
        partitioner = null;
    }

    public void shutdownNow(){
        //先关闭执行线程实例再关闭线程池
        //关闭并移除分区执行线程实例,且缓存
        for(PartitionTask task: partitionTasks){
            task.close();
        }
        threadPool.shutdownNow();
        //help gc
        partitionTasks = null;
        threadPool = null;
        partitioner = null;
    }

    public void expandTo(int newPartitionNum){
        Preconditions.checkArgument(newPartitionNum > partitionNum, "param newPartitionNum '{}' must be greater than maxPartition '{}'", newPartitionNum, partitionNum);

        //对partitionTasks加锁并扩容, 然后更新numPartition
        //这样能保证一致性, 并且不会发生IndexOutOfBound
        synchronized (partitionTasks){
            partitionTasks = Arrays.copyOf(partitionTasks, newPartitionNum);
            partitionNum = newPartitionNum;
            threadPool.setCorePoolSize(partitionNum);
        }
    }

    public void expand(int addPartitionNum){
        int newPartitionNum = partitionNum + addPartitionNum;
        expandTo(newPartitionNum);
    }

    public void shrink(int reducePartitionNum){
        int newPartitionNum = partitionNum - reducePartitionNum;
        shrinkTo(newPartitionNum);
    }

    private void shutdownTask(int num){
        Preconditions.checkArgument(num > 0, "the number of tasks need to be shutdowned must be positive");
        List<PartitionTask> removedPartitionTasks = new ArrayList<>();
        //关闭并移除分区执行线程实例,且缓存
        for(int i = partitionTasks.length - num; i < partitionTasks.length; i++){
            PartitionTask task = partitionTasks[i];
            if(task != null){
                partitionTasks[i] = null;
                task.close();
                removedPartitionTasks.add(task);
            }
        }

        //Executors doesn't shutdown
        if(!removedPartitionTasks.isEmpty()){
            for(PartitionTask partitionTask: removedPartitionTasks){
                while (!partitionTask.isTerminated){
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        ExceptionUtils.log(e);
                    }
                }
                //重新执行被移除线程但还没执行的tasks
                for(Task queuedTask: partitionTask.queue) {
                    execute(queuedTask.key, queuedTask.target);
                }

            }
        }
    }

    public void shrinkTo(int newPartitionNum){
        Preconditions.checkArgument(newPartitionNum > 0, "param newPartitionNum '{}' can't be zero or negative", newPartitionNum);
        Preconditions.checkArgument(newPartitionNum < partitionNum, "param newPartitionNum '{}' must be lower than nowPartitionNum '{}'", newPartitionNum, partitionNum);

        //对partitionTasks加锁并处理, 然后更新numPartition, 最后更新partitionTasks大小
        //这样能保证一致性, 并且不会发生数组index异常(因数组长度缩小)
        synchronized (partitionTasks) {
            int originPartitionNum = partitionNum;
            partitionNum = newPartitionNum;
            shutdownTask(originPartitionNum - partitionNum);
            partitionTasks = Arrays.copyOf(partitionTasks, partitionNum);
            threadPool.setCorePoolSize(newPartitionNum);
        }
    }

    private class Task implements Runnable{
        //缓存分区key,以便重分区时获取分区key
        private final K key;
        private final Runnable target;

        Task(K key, Runnable target) {
            this.key = key;
            this.target = target;
        }

        @Override
        public void run() {
            target.run();
        }
    }

    /**
     * task 执行
     */
    private class PartitionTask implements Runnable{
        //任务队列
        private BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
        //绑定的线程
        private Thread bind;

        private boolean isStopped = false;
        private boolean isTerminated = false;

        public void execute(Task task) {
            try {
                queue.put(task);
            } catch (InterruptedException e) {
                ExceptionUtils.log(e);
            }
        }

        public void close(){
            isStopped = true;
            if(bind != null){
                bind.interrupt();
            }
        }

        public void run() {
            bind = Thread.currentThread();
            while(!isStopped && !Thread.currentThread().isInterrupted()){
                Task task = null;
                try {
                    task = queue.take();
                    task.run();
                } catch (InterruptedException e) {
                    //执行中的task重新插入队头
                    if (task != null) {
                        queue.offer(task);
                    }
                } catch (Exception e) {
                    ExceptionUtils.log(e);
                }
            }
            isTerminated = true;
        }
    }
}
