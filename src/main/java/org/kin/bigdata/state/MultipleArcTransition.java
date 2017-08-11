package org.kin.bigdata.state;

/**
 * Created by 健勤 on 2017/8/9.
 */
public interface MultipleArcTransition<OPERAND, EVENT, STATE extends Enum<STATE>> {
    STATE transition(OPERAND operand, EVENT event);
}
