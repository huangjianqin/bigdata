package org.bigdata.kafka.multithread;

/**
 * Created by 健勤 on 2017/7/21.
 */
public class ClassUtil {
    /**
     * 通过无参构造器实例化类
     * @param claxx
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> T instance(Class<T> claxx) throws IllegalAccessException, InstantiationException {
        if(claxx == null){
            return null;
        }
        return claxx.newInstance();
    }

    /**
     * 根据参数调用构造器实例化类
     * @param claxx
     * @param args
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> T instance(Class<T> claxx, Object... args) throws IllegalAccessException, InstantiationException {
        if(claxx == null){
            return null;
        }
        return claxx.newInstance();
    }
}
