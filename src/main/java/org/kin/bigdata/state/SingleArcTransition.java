package org.kin.bigdata.state;

/**
 * Created by 健勤 on 2017/8/9.
 */
public interface SingleArcTransition<OPERAND, EVENT> {
    void transition(OPERAND operand, EVENT event);
}
