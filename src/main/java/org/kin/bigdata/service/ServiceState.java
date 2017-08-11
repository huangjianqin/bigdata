package org.kin.bigdata.service;

/**
 * Created by 健勤 on 2017/8/8.
 */
public class ServiceState {
    private String serviceName;
    private volatile Service.State state;
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

    public boolean isInState(Service.State proposed){
        return state.equals(proposed);
    }

    public synchronized Service.State enterState(Service.State post){
        checkStateTransition(serviceName, state, post);
        Service.State old = state;
        state = post;
        return old;
    }

    public static void checkStateTransition(String serviceName, Service.State pre, Service.State post){
        if(!isValidStateTransition(pre, post)){
            throw new IllegalStateException(serviceName + " can not enter state " + post.getStateName() + " from state " + pre.getStateName());
        }
    }

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