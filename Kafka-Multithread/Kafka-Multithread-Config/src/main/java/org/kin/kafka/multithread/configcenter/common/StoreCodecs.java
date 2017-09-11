package org.kin.kafka.multithread.configcenter.common;

import org.kin.kafka.multithread.utils.ClassUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class StoreCodecs {
    private static Map<StoreType, StoreCodec> storeCodecMap = new HashMap<>();

    static {
        for(StoreType storeType: StoreType.values()){
            storeCodecMap.put(storeType, (StoreCodec) ClassUtils.instance(String.format(StoreCodec.classpath, storeType.getTypeDesc())));
        }
    }

    public static StoreCodec getCodecByName(String storeType){
        return storeCodecMap.get(StoreType.valueOf(storeType));
    }

    public static StoreCodec getCodecByType(StoreType storeType){
        return storeCodecMap.get(storeType);
    }

    public enum StoreType {
        JOSN("Json"), YAML("Yaml"), PROPERTIES("Properties");

        private String typeDesc;

        StoreType(String typeDesc) {
            this.typeDesc = typeDesc;
        }

        public static StoreType getByDesc(String typeDesc){
            for(StoreType type: values()){
                if(type.getTypeDesc().toLowerCase().equals(typeDesc.toLowerCase())){
                    return type;
                }
            }
            return null;
        }

        public String getTypeDesc() {
            return typeDesc;
        }

        public void setTypeDesc(String typeDesc) {
            this.typeDesc = typeDesc;
        }
    }
}
