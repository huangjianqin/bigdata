package org.kin.jraft.springboot.counter.server;

import org.kin.framework.JvmCloseCleaner;
import org.kin.framework.concurrent.ExecutionContext;

import java.util.concurrent.ExecutorService;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterContext {
    public static final ExecutorService EXECUTOR = ExecutionContext.fix(10, "read-index");

    static {
        JvmCloseCleaner.instance().add(EXECUTOR::shutdown);
    }
}
