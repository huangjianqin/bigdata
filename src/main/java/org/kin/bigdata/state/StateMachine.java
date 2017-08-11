package org.kin.bigdata.state;

/**
 * Created by 健勤 on 2017/8/9.
 */
public interface StateMachine <STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    STATE getCurrentState();
    STATE doTransition(EVENTTYPE eventType, EVENT event);
}
