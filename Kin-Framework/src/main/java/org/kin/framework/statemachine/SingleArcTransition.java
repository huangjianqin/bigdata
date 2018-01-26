package org.kin.framework.statemachine;

/**
 * Created by 健勤 on 2017/8/9.
 * 一对一状态转换逻辑处理
 */
@FunctionalInterface
public interface SingleArcTransition<OPERAND, EVENT> {
    void transition(OPERAND operand, EVENT event);
}
