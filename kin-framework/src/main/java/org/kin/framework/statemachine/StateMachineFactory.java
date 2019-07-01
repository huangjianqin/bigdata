package org.kin.framework.statemachine;

import com.google.common.collect.Maps;
import org.kin.framework.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.TypeVariable;
import java.util.*;

/**
 * Created by 健勤 on 2017/8/9.
 * 状态机工厂类
 * 延迟构造状态拓扑图
 * <p>
 * OPERAND 该状态机的状态转换处理实例
 * STATE 该状态机的状态类型
 * EVENTTYPE 触发该状态机状态转换的事件类型
 * EVENT 触发该状态机状态转换的事件类
 */
public class StateMachineFactory<OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    private static Logger log = LoggerFactory.getLogger(StateMachine.class);

    //拓扑
    private Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> stateMachineTable;

    //初始状态
    private STATE defaultInitialState;

    //临时存储链表,最终会转换为拓扑
    //栈形式
    private final TransitionsListNode node;

    private String stateGraph;

    public StateMachineFactory(STATE defaultInitialState) {
        this.defaultInitialState = defaultInitialState;
        this.node = null;
        this.stateMachineTable = null;
    }

    /**
     * 用已有状态机构建新的状态机
     */
    private StateMachineFactory(
            StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
            ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> t) {
        this.defaultInitialState = that.defaultInitialState;
        this.node = new TransitionsListNode(t, that.node);
        this.stateMachineTable = null;
    }

    /**
     * 生成最终的状态机并构建拓扑
     */
    private StateMachineFactory(
            StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that) {
        this.defaultInitialState = that.defaultInitialState;
        this.node = that.node;
        //构建状态拓扑
        constructStateMachineTable();
    }

    /**
     * 生成状态图的接口
     */
    private interface ApplicableTransition<OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
        void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject);
    }

    /**
     * 状态表节点,最后表现成状态表的链表形式
     */
    private class TransitionsListNode {
        private final ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition;
        private final TransitionsListNode next;

        public TransitionsListNode(ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition, TransitionsListNode next) {
            this.transition = transition;
            this.next = next;
        }
    }

    /**
     * 存储链表每个节点具体生成拓扑的逻辑
     */
    private static class ApplicableSingleOrMultipleTransition<OPERAND, STATE extends Enum<STATE>, EVENTTYPE
            extends Enum<EVENTTYPE>, EVENT> implements ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> {
        //前驱状态
        private final STATE pre;
        //触发事件
        private final EVENTTYPE eventType;
        //具体事件处理
        private final Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition;

        public ApplicableSingleOrMultipleTransition(STATE pre, EVENTTYPE eventType, Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition) {
            this.pre = pre;
            this.eventType = eventType;
            this.transition = transition;
        }

        /**
         * 状态节点转换为拓扑
         */
        @Override
        public void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
            Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap = subject.stateMachineTable.get(pre);
            if (transitionMap == null) {
                //用HashMap性能更好
                transitionMap = subject.stateMachineTable.putIfAbsent(pre, Maps.newHashMap());
            }
            transitionMap.put(eventType, transition);
        }
    }

    /**
     * 内部状态转换逻辑抽象
     *
     * @param <OPERAND>
     * @param <STATE>
     * @param <EVENTTYPE>
     * @param <EVENT>
     */
    private interface Transition<OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
        STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType);

        Set<STATE> getPostStates();
    }

    /**
     * 内部一对一状态转换逻辑抽象
     * 本质上是对真正处理状态转换的实例进行再一层封装
     * 无需校验最后状态是否合法
     */
    private class SingleInternalArc implements Transition<OPERAND, STATE, EVENTTYPE, EVENT> {
        private STATE postState;
        private SingleArcTransition<OPERAND, EVENT> hook;

        public SingleInternalArc(STATE postState, SingleArcTransition<OPERAND, EVENT> hook) {
            this.postState = postState;
            this.hook = hook;
        }

        @Override
        public STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType) {
            if (hook != null) {
                hook.transition(operand, event);
            }
            log.info("state transition from {} to {}", oldState, postState);
            return postState;
        }

        @Override
        public Set<STATE> getPostStates() {
            return Collections.singleton(postState);
        }
    }

    /**
     * 内部一对多状态转换抽象
     * 本质上是对真正处理状态转换的实例进行再一层封装
     */
    private class MultipleInternalArc implements Transition<OPERAND, STATE, EVENTTYPE, EVENT> {
        private Set<STATE> validPostStates;
        private MultipleArcTransition<OPERAND, EVENT, STATE> hook;

        public MultipleInternalArc(Set<STATE> validPostStates, MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
            this.validPostStates = validPostStates;
            this.hook = hook;
        }

        /**
         * 校验最后状态是否合法
         */
        @Override
        public STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType) {
            STATE postState = hook.transition(operand, event);

            if (!validPostStates.contains(postState)) {
                throw new IllegalStateException("invalid state: " + postState + " transitioned from event " + event);
            }
            log.info("state transition from {} to {}", oldState, postState);
            return postState;
        }

        @Override
        public Set<STATE> getPostStates() {
            return validPostStates;
        }
    }

    /**
     * 内部状态机接口的实现
     * 构造时自动初始化factory的状态图
     */
    private class InternalStateMachine implements StateMachine<STATE, EVENTTYPE, EVENT> {
        private final OPERAND operand;
        private STATE currentState;

        public InternalStateMachine(OPERAND operand, STATE initialState) {
            this.operand = operand;
            this.currentState = initialState;
        }

        @Override
        public STATE getCurrentState() {
            return currentState;
        }

        /**
         * @return 转换后的状态
         */
        @Override
        public STATE doTransition(EVENTTYPE eventType, EVENT event) {
            currentState = StateMachineFactory.this.doTransition(operand, currentState, eventType, event);
            return currentState;
        }
    }

    /**
     * 状态直接转换
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, EVENTTYPE eventType) {
        return addTransition(pre, post, eventType, null);
    }

    /**
     * 一对一状态转换
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, EVENTTYPE eventType,
                                                                               SingleArcTransition<OPERAND, EVENT> hook) {
        return new StateMachineFactory<>(this, new ApplicableSingleOrMultipleTransition<>(pre, eventType, new SingleInternalArc(post, hook)));
    }

    /**
     * 多个事件可触发同一的一对一状态转换
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, Set<EVENTTYPE> eventTypes,
                                                                               SingleArcTransition<OPERAND, EVENT> hook) {
        StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory = null;
        for (EVENTTYPE eventType : eventTypes) {
            if (factory == null) {
                factory = addTransition(pre, post, eventType, hook);
            } else {
                factory = factory.addTransition(pre, post, eventType, hook);
            }
        }
        return factory;
    }

    /**
     * 多个事件可触发同一状态直接转换
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, Set<EVENTTYPE> eventTypes) {
        return addTransition(pre, post, eventTypes, null);
    }

    /**
     * 一对多状态转换
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, Set<STATE> posts, EVENTTYPE eventType,
                                                                               MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
        return new StateMachineFactory<>(this, new ApplicableSingleOrMultipleTransition<>(pre, eventType, new MultipleInternalArc(posts, hook)));
    }

    /**
     * 利用状态表链表构造状态图
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> installTopology() {
        return new StateMachineFactory<>(this);
    }

    /**
     * 状态转换逻辑处理细节
     */
    private STATE doTransition(OPERAND operand, STATE old, EVENTTYPE eventType, EVENT event) {
        Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap = stateMachineTable.get(old);
        if (transitionMap != null) {
            //本质上两个内部类
            Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = transitionMap.get(eventType);
            if (transition != null) {
                //其真正调用的是MultipleArcTransition和SingleARCTransition接口
                return transition.doTransition(operand, old, event, eventType);
            }
        }

        throw new IllegalStateException("can't transition from state " + old + " when hit event " + event);
    }

    /**
     * 构造状态图
     */
    private void constructStateMachineTable() {
        Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>> stack = new Stack<>();
        Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> prototype = new HashMap<>();
        prototype.put(defaultInitialState, null);
        //这里用EnumMap使得数据结构更加紧凑,性能更好
        stateMachineTable = new EnumMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);

        //按方法调用顺序构建拓扑
        //ps:因为这里的链表构建类似栈
        for (TransitionsListNode cursor = node; cursor != null; cursor = cursor.next) {
            stack.push(cursor.transition);
        }

        while (!stack.empty()) {
            stack.pop().apply(this);
        }
    }

    /**
     * 根据OPERAND operand, STATE initialState构造状态机
     */
    public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand, STATE initialState) {
        return new InternalStateMachine(operand, initialState);
    }

    public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand) {
        return make(operand, defaultInitialState);
    }

    /**
     * 生成可视化图
     */
    public void generateStateGraph() {
        if (StringUtils.isBlank(stateGraph)) {
            StringBuilder sb = new StringBuilder();
            sb.append("@@@@@@@@@ state  machine @@@@@@@@@").append(System.lineSeparator());
            sb.append("InitialState: ").append(defaultInitialState);
            sb.append("handler: ").append(getClass().getTypeParameters()[0].getName());
            for (Map.Entry<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> entry1 : stateMachineTable.entrySet()) {
                STATE pre = entry1.getKey();
                Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> value = entry1.getValue();

                sb.append(String.format("######from state '%s'######", pre)).append(System.lineSeparator());
                for (Map.Entry<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> entry2 : value.entrySet()) {
                    EVENTTYPE eventtype = entry2.getKey();
                    Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = entry2.getValue();

                    TypeVariable<? extends Class<? extends Transition>>[] typeParameter = transition.getClass().getTypeParameters();

                    sb.append(String.format("trigger eventType: '%s'", eventtype)).append(System.lineSeparator());
                    sb.append(String.format("trigger event: '%s'", typeParameter[3].getName())).append(System.lineSeparator());
                    sb.append("to state: ");
                    for (STATE postState : transition.getPostStates()) {
                        sb.append(postState).append(",");
                    }
                    sb.append(System.lineSeparator());
                }
                sb.append("#########  end  ###########").append(System.lineSeparator());

            }
            sb.append("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@").append(System.lineSeparator());
            this.stateGraph = sb.toString();
        }
        System.out.println(this.stateGraph);
    }

}
