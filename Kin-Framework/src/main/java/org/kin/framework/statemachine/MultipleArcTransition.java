package org.kin.framework.statemachine;

/**
 * Created by 健勤 on 2017/8/9.
 * 一对多状态转换逻辑处理
 */
@FunctionalInterface
public interface MultipleArcTransition<OPERAND, EVENT, STATE extends Enum<STATE>> {
    STATE transition(OPERAND operand, EVENT event);
}
