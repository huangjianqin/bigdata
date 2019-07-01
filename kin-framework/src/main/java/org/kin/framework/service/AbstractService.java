package org.kin.framework.service;

import org.kin.framework.utils.StringUtils;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by 健勤 on 2017/8/8.
 * 服务抽象
 */
public abstract class AbstractService implements Service{
    private final String serviceName;
    private final ServiceState state;
    private long startTime;
    private final List<ServiceStateChangeListener> listeners = new LinkedList<>();
    private static final List<ServiceStateChangeListener> GLOBAL_LISTENERS = new LinkedList<>();

    //
    private final Object lock = new Object();
    private final AtomicBoolean terminationNotification = new AtomicBoolean(false);

    public AbstractService() {
        this("");
    }

    public AbstractService(String serviceName) {
        if (StringUtils.isNotBlank(serviceName)) {
            this.serviceName = serviceName;
        } else {
            this.serviceName = getClass().getSimpleName();
        }
        //服务初始状态
        this.state = new ServiceState(serviceName, State.NOTINITED);
    }

    @Override
    public void init() {
        if (isInState(State.INITED)) {
            return;
        }

        synchronized (lock) {
            State pre = state.enterState(State.INITED);
            if (pre != State.INITED) {
                serviceInit();
                notifyAllListeners(pre);
//                //再次判断
//                if(isInState(State.INITED)){
//                    notifyAllListeners(pre);
//                }
            }
        }
    }

    @Override
    public void start() {
        if (isInState(State.STARTED)) {
            return;
        }

        synchronized (lock) {
            State pre = state.enterState(State.STARTED);
            if (pre != State.STARTED) {
                startTime = System.currentTimeMillis();
                serviceStart();
                notifyAllListeners(pre);

//                //再次判断
//                if(isInState(State.STARTED)){
//                    notifyAllListeners(pre);
//                }
            }
        }
    }

    @Override
    public void stop() {
        if (isInState(State.STOPPED)) {
            return;
        }

        synchronized (lock) {
            State pre = state.enterState(State.STOPPED);
            if (pre != State.STOPPED) {
                serviceStop();
                notifyAllListeners(pre);

                terminationNotification.set(true);
                synchronized (terminationNotification) {
                    terminationNotification.notifyAll();
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    @Override
    public final boolean waitForServiceToStop(long millis) {
        boolean isStopped = terminationNotification.get();
        if (!isStopped) {
            try {
                synchronized (terminationNotification) {
                    terminationNotification.wait(millis);
                }
            } catch (InterruptedException e) {
                return terminationNotification.get();
            }
        }

        return terminationNotification.get();
    }

    @Override
    public void registerServiceListener(ServiceStateChangeListener listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterServiceListener(ServiceStateChangeListener listener) {
        listeners.remove(listener);
    }

    @Override
    public boolean isInState(State that) {
        return state.isInState(that);
    }

    @Override
    public String getName() {
        return serviceName;
    }

    @Override
    public State getCurrentState() {
        return state.getState();
    }

    @Override
    public long getStartTime() {
        return startTime;
    }

    protected void serviceInit() {
    }

    protected void serviceStart() {
    }

    protected void serviceStop() {
    }

    private void notifyAllListeners(State pre) {
        notifyListeners(listeners, pre);
        notifyListeners(GLOBAL_LISTENERS, pre);
    }

    private void notifyListeners(Collection<ServiceStateChangeListener> listeners, State pre) {
        for (ServiceStateChangeListener listener : listeners) {
            listener.onStateChanged(this, pre);
        }
    }

    public void registerGlogalListener(ServiceStateChangeListener listener) {
        GLOBAL_LISTENERS.add(listener);
    }

    public void unregisterGlogalListener(ServiceStateChangeListener listener) {
        GLOBAL_LISTENERS.remove(listener);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "serviceName='" + serviceName + '\'' +
                '}';
    }
}