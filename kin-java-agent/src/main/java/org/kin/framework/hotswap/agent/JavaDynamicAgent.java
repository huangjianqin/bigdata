package org.kin.framework.hotswap.agent;

import java.lang.instrument.Instrumentation;

/**
 * Created by huangjianqin on 2018/2/3.
 * 必须单独打包，并且MANNIFEST.MF设置
 * Agent-Class: JavaDynamicAgent.jar
 * Can-Redefine-Classes: true
 */
public class JavaDynamicAgent {
    private static Instrumentation instrumentation;
    private static Object lockObject = new Object();

    /**
     * 方法必须叫agentmain
     */
    public static void agentmain(String args, Instrumentation inst) {
        synchronized (lockObject) {
            if (instrumentation == null) {
                instrumentation = inst;
            }
        }
    }

    public static Instrumentation getInstrumentation() {
        return instrumentation;
    }

    public static long getObjectSize(Object o) {
        return instrumentation.getObjectSize(o);
    }
}
