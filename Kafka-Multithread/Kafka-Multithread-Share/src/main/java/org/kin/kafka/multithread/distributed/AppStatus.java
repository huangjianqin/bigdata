package org.kin.kafka.multithread.distributed;

/**
 * Created by huangjianqin on 2017/9/19.
 * 与配置中心AppStatus枚举一致
 */
public enum AppStatus {
    RUN("RUN"), UPDATE("UPDATE"), CLOSE("CLOSE"), RESTART("RESTART");

    private String statusDesc;

    AppStatus(String statusDesc) {
        this.statusDesc = statusDesc;
    }

    public String getStatusDesc() {
        return statusDesc;
    }

    public static AppStatus getByStatusDesc(String statusDesc){
        for(AppStatus appStatus : values()){
            if(appStatus.getStatusDesc().equals(statusDesc.toUpperCase())){
                return appStatus;
            }
        }
        throw new IllegalStateException("unsupport AppStatus '" + statusDesc + "'");
    }
}
