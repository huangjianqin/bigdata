package org.kin.framework.statemachine;

/**
 * Created by 健勤 on 2017/8/9.
 * 状态机接口
 */
public interface StateMachine<STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    STATE getCurrentState();

    STATE doTransition(EVENTTYPE eventType, EVENT event);
}
