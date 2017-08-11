package org.kin.bigdata.state;

/**
 * Created by 健勤 on 2017/8/9.
 * 一对一状态转换,无需校验最后状态是否合法
 */
public interface SingleArcTransition<OPERAND, EVENT> {
    void transition(OPERAND operand, EVENT event);
}
