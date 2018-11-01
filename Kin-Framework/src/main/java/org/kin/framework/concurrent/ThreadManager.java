package org.kin.framework.concurrent;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.kin.framework.utils.Constants;
import org.kin.framework.utils.SysUtils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2018/1/24.
 */
public class ThreadManager implements ScheduledExecutorService {
    public static ThreadManager DEFAULT;

    static {
        String executorTypeStr = System.getenv(Constants.DEFAULT_EXECUTOR);
        if (!Strings.isNullOrEmpty(executorTypeStr)) {
            ExecutorType executorType = ExecutorType.getByName(executorTypeStr);
            DEFAULT = new ThreadManager(executorType.getExecutor());
        } else {
            DEFAULT = new ThreadManager();
        }
    }

    //执行线程
    private ExecutorService executor = ExecutorType.THREADPOOL.getExecutor();
    //调度线程
    private ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(SysUtils.getSuitableThreadNum());

    private ThreadManager() {
        hook();
    }

    public ThreadManager(ExecutorService executor) {
        this();
        this.executor = executor;
    }

    public ThreadManager(ScheduledExecutorService scheduleExecutor) {
        this();
        this.scheduleExecutor = scheduleExecutor;
    }

    public ThreadManager(ExecutorService executor, ScheduledExecutorService scheduleExecutor) {
        this();
        this.executor = executor;
        this.scheduleExecutor = scheduleExecutor;
    }

    private void hook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!isShutdown()) {
                shutdownNow();
            }
        }));
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return scheduleExecutor.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return scheduleExecutor.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return scheduleExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return scheduleExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    @Override
    public void shutdown() {
        executor.shutdown();
        scheduleExecutor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> tasks = Lists.newArrayList();
        tasks.addAll(executor.shutdownNow());
        tasks.addAll(scheduleExecutor.shutdownNow());
        return tasks;
    }

    @Override
    public boolean isShutdown() {
        return executor.isShutdown() && scheduleExecutor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return executor.isTerminated() && scheduleExecutor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return executor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return executor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return executor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return executor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }
}
