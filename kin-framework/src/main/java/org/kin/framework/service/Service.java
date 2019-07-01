package org.kin.framework.service;

import org.kin.framework.Closeable;

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
        /**
         * 服务未初始化状态
         */
        NOTINITED(0, "NOTINITED"),
        /**
         * 服务已初始化状态
         */
        INITED(1, "INITED"),
        /**
         * 服务已启动
         */
        STARTED(2, "STARTED"),
        /**
         * 服务已停止
         */
        STOPPED(3, " STOPPED");

        private final int stateId;
        private final String stateName;

        State(int stateId, String stateName) {
            this.stateId = stateId;
            this.stateName = stateName;
        }

        static State getById(int stateId) {
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

        static State getByName(String stateName) {
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

        int getStateId() {
            return stateId;
        }

        String getStateName() {
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
