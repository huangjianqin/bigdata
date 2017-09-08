package org.kin.bigdata.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by huangjianqin on 2017/9/5.
 */
public class ReflectUtils {
    public static <T> T instance(Class<T> claxx, Object... args){
        if(claxx != null){
            try {
                if(args.length == 0){
                    return claxx.newInstance();
                }
                Class[] argClasses = new Class[args.length];
                for(int i = 0; i < argClasses.length; i++){
                    argClasses[i] = args[i].getClass();
                }
                Constructor<T> constructor = claxx.getConstructor(argClasses);
                return constructor.newInstance(args);
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
