package org.bigdata.kafka.multithread;

/**
 * Created by 健勤 on 2017/7/21.
 */
public class ClassUtil {
    public static <T> T instance(Class<T> claxx) throws IllegalAccessException, InstantiationException {
        if(claxx == null){
            return null;
        }
        return claxx.newInstance();
    }

    public static <T> T instance(Class<T> claxx, Object... args) throws IllegalAccessException, InstantiationException {
        if(claxx == null){
            return null;
        }
        return claxx.newInstance();
    }
}
