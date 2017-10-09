package org.kin.framework.service;

/**
 * Created by 健勤 on 2017/8/8.
 * 某一具体服务状态的类
 */
public class ServiceState {
    private String serviceName;
    private volatile Service.State state;
    //服务状态转换规则
    private static final boolean[][] stateMap = {
            //             notInited inited started stopped
            /*notInited*/ {false, true, false, true},
            /*inite     */ {false, true, true, true},
            /*started   */ {false, false, true, true},
            /*stopped   */ {false, false, false, true}
    };

    public ServiceState(String serviceName) {
        this.serviceName = serviceName;
    }

    public ServiceState(String serviceName, Service.State state) {
        this.serviceName = serviceName;
        this.state = state;
    }

    /**
     * 当前状态是否是指定状态
     * @param proposed
     * @return
     */
    public boolean isInState(Service.State proposed){
        return state.equals(proposed);
    }

    /**
     * 进入状态
     * @param post
     * @return
     */
    public synchronized Service.State enterState(Service.State post){
        //检查状态转换是否合法
        checkStateTransition(serviceName, state, post);
        Service.State old = state;
        state = post;
        return old;
    }

    /**
     * 状态转换不合法则抛出异常
     * @param serviceName
     * @param pre
     * @param post
     */
    public static void checkStateTransition(String serviceName, Service.State pre, Service.State post){
        if(!isValidStateTransition(pre, post)){
            throw new IllegalStateException(serviceName + " can not enter state " + post.getStateName() + " from state " + pre.getStateName());
        }
    }

    /**
     * 根据规则判断状态转换是否合法
     * @param pre
     * @param post
     * @return
     */
    private static boolean isValidStateTransition(Service.State pre, Service.State post){
        boolean[] targetMap = stateMap[pre.getStateId()];
        return targetMap[post.getStateId()];
    }

    public Service.State getState() {
        return state;
    }

    public static boolean[][] getStateMap() {
        return stateMap;
    }
}