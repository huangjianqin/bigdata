package org.kin.framework.concurrent;

import com.google.common.collect.Lists;
import org.kin.framework.utils.Constants;
import org.kin.framework.utils.StringUtils;
import org.kin.framework.utils.SysUtils;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by huangjianqin on 2018/1/24.
 */
public class ThreadManager implements ScheduledExecutorService {
    public static ThreadManager DEFAULT;
    public static final ExecutorType DEFAULT_EXECUTOR_TYPE;

    static {
        String executorTypeStr = System.getenv(Constants.DEFAULT_EXECUTOR);
        if (StringUtils.isNotBlank(executorTypeStr)) {
            DEFAULT_EXECUTOR_TYPE = ExecutorType.getByName(executorTypeStr);
            DEFAULT = new ThreadManager(DEFAULT_EXECUTOR_TYPE.getExecutor(), getDefaultScheduledExecutor());
        } else {
            DEFAULT_EXECUTOR_TYPE = ExecutorType.THREADPOOL;
            DEFAULT = new ThreadManager(DEFAULT_EXECUTOR_TYPE.getExecutor(), getDefaultScheduledExecutor());
        }
    }

    //执行线程
    private ExecutorService executor;
    //调度线程
    private ScheduledExecutorService scheduleExecutor;

    private ThreadManager() {
    }

    public ThreadManager(ExecutorService executor) {
        this(executor, getDefaultScheduledExecutor());
    }

    public ThreadManager(ScheduledExecutorService scheduleExecutor) {
        this(DEFAULT_EXECUTOR_TYPE.getExecutor(), scheduleExecutor);
    }

    public ThreadManager(ExecutorService executor, ScheduledExecutorService scheduleExecutor) {
        this.executor = executor;
        this.scheduleExecutor = scheduleExecutor;
    }

    public static ThreadManager forkJoinPoolThreadManager() {
        return new ThreadManager(ExecutorType.FORKJOIN.getExecutor());
    }

    public static ThreadManager forkJoinPoolThreadManagerWithScheduled() {
        return new ThreadManager(
                ExecutorType.FORKJOIN.getExecutor(),
                Executors.newScheduledThreadPool(SysUtils.getSuitableThreadNum())
        );
    }

    public static ThreadManager CommonThreadManager() {
        return new ThreadManager(ExecutorType.THREADPOOL.getExecutor());
    }

    public static ThreadManager CommonThreadManagerWithScheduled() {
        return new ThreadManager(
                ExecutorType.THREADPOOL.getExecutor(),
                Executors.newScheduledThreadPool(SysUtils.getSuitableThreadNum())
        );
    }

    //--------------------------------------------------------------------------------------------

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

    //--------------------------------------------------------------------------------------------
    private enum ExecutorType {
        FORKJOIN("ForkJoin") {
            @Override
            public ExecutorService getExecutor() {
                return new ForkJoinPool();
            }
        },
        THREADPOOL("ThreadPool") {
            @Override
            public ExecutorService getExecutor() {
                return Executors.newFixedThreadPool(SysUtils.getSuitableThreadNum(), new SimpleThreadFactory("default-thread-manager"));
            }
        };
        private String name;

        ExecutorType(String name) {
            this.name = name;
        }

        public abstract ExecutorService getExecutor();

        public static ExecutorType getByName(String name) {
            for (ExecutorType type : values()) {
                if (type.getName().toLowerCase().equals(name.toLowerCase())) {
                    return type;
                }
            }

            throw new UnknownExecutorTypeException("unknown executor type '" + name + "'");
        }

        public String getName() {
            return name;
        }

        private static class UnknownExecutorTypeException extends RuntimeException {
            public UnknownExecutorTypeException(String message) {
                super(message);
            }
        }
    }

    static ScheduledExecutorService getDefaultScheduledExecutor(){
        return Executors.newScheduledThreadPool(SysUtils.getSuitableThreadNum());
    }
}
