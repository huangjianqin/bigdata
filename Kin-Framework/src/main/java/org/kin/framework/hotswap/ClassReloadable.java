package org.kin.framework.hotswap;

import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.ExceptionUtils;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangjianqin on 2018/2/1.
 * 类热更新思路->加载新的Class,重新构建实例,把所有引用了该Class的地方重新加载Class(不断传播下去)
 */
public abstract class ClassReloadable implements Reloadable{
    /**
     * 默认热更新实现，仅仅会替换当前类的成员接口实现实例(定义类型是interface)
     *
     ***目前无法处理类的热更替换,因为无法感知当前类在哪里被引用(如果我实现,相当于实现了spring的一部分,所以会有一个基于spring的热更实现)
     *
     * @param changedClass  新Class
     */
    void reload(Class<?> changedClass){
        List<Field> affectedFields = new ArrayList<>();
        Class<?> my = this.getClass();
        //不断往父类遍历,获取受影响的Field
        for(Field field: ClassUtils.getAllFields(my)){
            try {
                field.setAccessible(true);
                if (field.getType().isInterface() &&
                        field.getType().isAssignableFrom(changedClass) &&
                        field.get(this).getClass().getName().equals(changedClass.getName())) {
                    //接口
                    //changedClass实现了该接口
                    //实现类相同
                    affectedFields.add(field);
                }
            } catch (IllegalAccessException e) {
                ExceptionUtils.log(e);
            }
            finally {
                field.setAccessible(false);
            }
        }

        //替换实例
        for(Field field: affectedFields){
            try {
                field.setAccessible(true);
                Object newObj = changedClass.newInstance();
                field.set(this, newObj);
            } catch (IllegalAccessException e) {
                ExceptionUtils.log(e);
            } catch (InstantiationException e) {
                ExceptionUtils.log(e);
            }
            finally {
                field.setAccessible(false);
            }
        }
    }
}
