package org.kin.kafka.multithread.configcenter.codec;

import org.kin.kafka.multithread.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangjianqin on 2017/9/11.
 */
public class StoreCodecs {
    private static final Logger log = LoggerFactory.getLogger(StoreCodecs.class);

    private static Map<StoreType, StoreCodec> storeCodecMap = new HashMap<>();

    static {
        log.info("loading store codecs...");
        for(StoreType storeType: StoreType.values()){
            storeCodecMap.put(storeType, (StoreCodec) ClassUtils.instance(String.format(StoreCodec.classpath, storeType.getTypeDesc())));
            log.info("loaded " + storeType.getTypeDesc() + " store codec in classpath" + StoreCodec.classpath, storeType.getTypeDesc());
        }
        log.info("all available store codecs in classpath load");
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
