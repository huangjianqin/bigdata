package org.kin.framework.service;

import java.io.Closeable;

/**
 * Created by 健勤 on 2017/8/8.
 * 服务接口
 */
public interface Service extends Closeable {
    void init();

    void start();

    void stop();

    boolean waitForServiceToStop(long mills);

    void registerServiceListener(ServiceStateChangeListener listener);

    void unregisterServiceListener(ServiceStateChangeListener listener);

    /**
     * 当前状态是否是指定状态
     *
     * @param that
     * @return
     */
    boolean isInState(State that);

    String getName();

    State getCurrentState();

    long getStartTime();

    /**
     * 服务状态的枚举类
     */
    enum State {
        NOTINITED(0, "NOTINITED"),
        INITED(1, "INITED"),
        STARTED(2, "STARTED"),
        STOPPED(3, " STOPPED");

        private final int stateId;
        private final String stateName;

        State(int stateId, String stateName) {
            this.stateId = stateId;
            this.stateName = stateName;
        }

        public static State getById(int stateId) {
            if (stateId == NOTINITED.getStateId()) {
                return NOTINITED;
            } else if (stateId == INITED.getStateId()) {
                return INITED;
            } else if (stateId == STARTED.getStateId()) {
                return STARTED;
            } else if (stateId == STOPPED.getStateId()) {
                return STOPPED;
            } else {
                throw new IllegalStateException("unknown state id");
            }
        }

        public static State getByName(String stateName) {
            if (stateName.toUpperCase().equals(NOTINITED.getStateName())) {
                return NOTINITED;
            } else if (stateName.toUpperCase().equals(INITED.getStateName())) {
                return INITED;
            } else if (stateName.toUpperCase().equals(STARTED.getStateName())) {
                return STARTED;
            } else if (stateName.toUpperCase().equals(STOPPED.getStateName())) {
                return STOPPED;
            } else {
                throw new IllegalStateException("unknown state name");
            }
        }

        public int getStateId() {
            return stateId;
        }

        public String getStateName() {
            return stateName;
        }

        @Override
        public String toString() {
            return "Service.State{" +
                    "stateName='" + stateName + '\'' +
                    '}';
        }
    }
}
