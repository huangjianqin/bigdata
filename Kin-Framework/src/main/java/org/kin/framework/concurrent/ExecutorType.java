package org.kin.framework.concurrent;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

/**
 * Created by huangjianqin on 2018/2/25.
 */
public enum ExecutorType {
    FORKJOIN("ForkJoin") {
        @Override
        public ExecutorService getExecutor() {
            return new ForkJoinPool();
        }
    },
    THREADPOOL("ThreadPool") {
        @Override
        public ExecutorService getExecutor() {
            return Executors.newCachedThreadPool();
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
        public UnknownExecutorTypeException() {
        }

        public UnknownExecutorTypeException(String message) {
            super(message);
        }
    }


}
