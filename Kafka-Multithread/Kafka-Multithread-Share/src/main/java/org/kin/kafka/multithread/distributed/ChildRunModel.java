package org.kin.kafka.multithread.distributed;

/**
 * Created by huangjianqin on 2017/9/18.
 * 与配置中心ChildRunModel枚举一致
 */
public enum ChildRunModel {
    JVM("JVM"), NODE("NODE");

    private String model;

    ChildRunModel(String model) {
        this.model = model;
    }

    public String getModel() {
        return model;
    }

    public static ChildRunModel getByName(String name){
        for(ChildRunModel childRunModel: values()){
            if(name.toUpperCase().equals(childRunModel.getModel())){
                return childRunModel;
            }
        }
        throw new IllegalStateException("unknown child run model");
    }
}
