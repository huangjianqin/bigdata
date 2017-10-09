package org.kin.framework.state;

import java.util.*;

/**
 * Created by 健勤 on 2017/8/9.
 * 状态机工厂类
 * 延迟构造状态拓扑图
 *
 * OPERAND 该状态机的状态转换处理实例
 * STATE 该状态机的状态类型
 * EVENTTYPE 触发该状态机状态转换的事件类型
 * EVENT 触发该状态机状态转换的事件类
 */
public class StateMachineFactory <OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT>{

    private Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> stateMachineTable;

    private STATE defaultInitialState;

    private final TransitionsListNode node;

    public StateMachineFactory(STATE defaultInitialState) {
        this.defaultInitialState = defaultInitialState;
        this.node = null;
        this.stateMachineTable = null;
    }

    private StateMachineFactory(
            StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
            ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> t) {
        this.defaultInitialState = that.defaultInitialState;
        this.node = new TransitionsListNode(t, that.node);
        this.stateMachineTable = null;
    }

    private StateMachineFactory(
            StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that) {
        this.defaultInitialState = that.defaultInitialState;
        this.node = that.node;
        //构建状态拓扑
        constructStateMachineTable();
    }

    /**
     * 处理状态图的接口
     * @param <OPERAND>
     * @param <STATE>
     * @param <EVENTTYPE>
     * @param <EVENT>
     */
    private interface ApplicableTransition<OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT>{
        void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject);
    }

    /**
     * 状态表节点,最后表现成状态表的链表形式
     */
    private class TransitionsListNode{
        private final ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition;
        private final TransitionsListNode next;

        public TransitionsListNode(ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition, TransitionsListNode next) {
            this.transition = transition;
            this.next = next;
        }
    }

    /**
     *
     * @param <OPERAND>
     * @param <STATE>
     * @param <EVENTTYPE>
     * @param <EVENT>
     */
    private static class ApplicableSingleOrMultipleTransition<OPERAND, STATE extends Enum<STATE>, EVENTTYPE
            extends Enum<EVENTTYPE>, EVENT> implements ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT >{
        private final STATE pre;
        private final EVENTTYPE eventType;
        private final Transition<OPERAND, STATE, EVENTTYPE, EVENT > transition;

        public ApplicableSingleOrMultipleTransition(STATE pre, EVENTTYPE eventType, Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition) {
            this.pre = pre;
            this.eventType = eventType;
            this.transition = transition;
        }

        @Override
        public void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
            Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap = subject.stateMachineTable.get(pre);
            if(transitionMap == null){
                //用HashMap性能更好
                transitionMap = new HashMap<>();
                subject.stateMachineTable.put(pre, transitionMap);
            }
            transitionMap.put(eventType, transition);
        }
    }

    /**
     * 内部状态转换抽象
     * @param <OPERAND>
     * @param <STATE>
     * @param <EVENTTYPE>
     * @param <EVENT>
     */
    private interface Transition <OPERAND, STATE extends Enum<STATE>, EVENTTYPE extends Enum<EVENTTYPE>, EVENT>{
        STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType);
    }

    /**
     * 内部一对一状态转换抽象
     * 本质上是对真正处理状态转换的实例进行再一层封装
     */
    private class SingleInternalArc implements Transition<OPERAND, STATE, EVENTTYPE, EVENT>{
        private STATE postState;
        private SingleArcTransition<OPERAND, EVENT> hook;

        public SingleInternalArc(STATE postState, SingleArcTransition<OPERAND, EVENT> hook) {
            this.postState = postState;
            this.hook = hook;
        }

        @Override
        public STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType) {
            if(hook != null){
                hook.transition(operand, event);
            }
            return postState;
        }
    }

    /**
     * 内部一对多状态转换抽象
     * 本质上是对真正处理状态转换的实例进行再一层封装
     */
    private class MultipleInternalArc implements Transition<OPERAND, STATE, EVENTTYPE, EVENT>{
        private Set<STATE> validPostStates;
        private MultipleArcTransition<OPERAND, EVENT, STATE> hook;

        public MultipleInternalArc(Set<STATE> validPostStates, MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
            this.validPostStates = validPostStates;
            this.hook = hook;
        }

        @Override
        public STATE doTransition(OPERAND operand, STATE oldState, EVENT event, EVENTTYPE eventType) {
            STATE postState = hook.transition(operand, event);

            if(!validPostStates.contains(postState)){
                throw new IllegalStateException("invalid state: " + postState + " transitioned from event " + event);
            }

            return postState;
        }
    }

    /**
     * 内部状态机接口的实现
     * 构造时自动初始化factory的状态图
     */
    private class InternalStateMachine implements StateMachine<STATE, EVENTTYPE, EVENT>{
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

        @Override
        public STATE doTransition(EVENTTYPE eventType, EVENT event) {
            currentState = StateMachineFactory.this.doTransition(operand, currentState, eventType, event);
            return currentState;
        }
    }

    /**
     * 状态直接转换
     * @param pre
     * @param post
     * @param eventType
     * @return
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, EVENTTYPE eventType){
        return addTransition(pre, post, eventType, null);
    }

    /**
     * 一对一状态转换
     * @param pre
     * @param post
     * @param eventType
     * @param hook
     * @return
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, EVENTTYPE eventType,
                                                                               SingleArcTransition<OPERAND, EVENT> hook){
        return new StateMachineFactory<>(this, new ApplicableSingleOrMultipleTransition<>(pre, eventType, new SingleInternalArc(post, hook)));
    }

    /**
     * 多个事件可触发同一的一对一状态转换
     * @param pre
     * @param post
     * @param eventTypes
     * @param hook
     * @return
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, Set<EVENTTYPE> eventTypes,
                                                                               SingleArcTransition<OPERAND, EVENT> hook){
        StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory = null;
        for(EVENTTYPE eventType: eventTypes){
            if(factory == null){
                factory = addTransition(pre, post, eventType, hook);
            }
            else{
                factory = factory.addTransition(pre, post, eventType, hook);
            }
        }
        return factory;
    }

    /**
     * 多个事件可触发同一状态直接转换
     * @param pre
     * @param post
     * @param eventTypes
     * @return
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, STATE post, Set<EVENTTYPE> eventTypes){
        return addTransition(pre, post, eventTypes, null);
    }

    /**
     * 一对多状态转换
     * @param pre
     * @param posts
     * @param eventType
     * @param hook
     * @return
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(STATE pre, Set<STATE> posts, EVENTTYPE eventType,
                                                                               MultipleArcTransition<OPERAND, EVENT, STATE> hook){
       return new StateMachineFactory<>(this, new ApplicableSingleOrMultipleTransition<>(pre, eventType, new MultipleInternalArc(posts, hook)));
    }

    /**
     * 利用状态表链表构造状态图
     * @return
     */
    public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> installTopology(){
        return new StateMachineFactory<>(this);
    }

    /**
     * 状态转换逻辑处理细节
     * @param operand
     * @param old
     * @param eventType
     * @param event
     * @return
     */
    private STATE doTransition(OPERAND operand, STATE old, EVENTTYPE eventType, EVENT event){
        Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap = stateMachineTable.get(old);
        if(transitionMap != null){
            //本质上两个内部类
            Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = transitionMap.get(eventType);
            if(transition != null){
                //其真正调用的是MultipleArcTransition和SingleARCTransition接口
                return transition.doTransition(operand, old, event, eventType);
            }
        }

        throw new IllegalStateException("can't transition from state " + old + " when hit event " + event);
    }

    /**
     * 构造状态图
     */
    private void constructStateMachineTable(){
        Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>> stack = new Stack<>();
        Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> prototype = new HashMap<>();
        prototype.put(defaultInitialState, null);
        //这里用EnumMap使得数据结构更加紧凑,性能更好
        stateMachineTable = new EnumMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);

        for(TransitionsListNode cursor = node; cursor != null; cursor = cursor.next){
            stack.push(cursor.transition);
        }

        while(!stack.empty()){
            stack.pop().apply(this);
        }
    }

    /**
     * 根据OPERAND operand, STATE initialState构造状态机
     * @param operand
     * @param initialState
     * @return
     */
    public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand, STATE initialState){
        return new InternalStateMachine(operand, initialState);
    }

    public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand){
        return make(operand, defaultInitialState);
    }

    /**
     * 生成可视化图
     */
    public void generateStateGraph(){

    }

}
