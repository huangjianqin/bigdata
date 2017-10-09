package org.kin.framework.state;

/**
 * Created by 健勤 on 2017/8/9.
 * 一对多状态转换,需校验最后状态是否合法
 */
public interface MultipleArcTransition<OPERAND, EVENT, STATE extends Enum<STATE>> {
    STATE transition(OPERAND operand, EVENT event);
}
