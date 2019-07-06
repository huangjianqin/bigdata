//package org.kin.framework.hotswap.deadcode;
//
//import org.kin.framework.hotswap.Reloadable;
//import org.kin.framework.utils.ClassUtils;
//import org.kin.framework.utils.ExceptionUtils;
//
//import java.lang.reflect.Field;
//
///**
// * Created by huangjianqin on 2018/2/1.
// */
//public abstract class ClassReloadableEntity implements Reloadable {
//    /**
//     * 默认热更新实现，仅仅会替换当前类(包括子类和父类)的成员域
//     * <p>
//     * **目前无法处理类的热更替换,因为无法感知当前类在哪里被引用(如果我实现,相当于实现了spring的一部分,所以会有一个基于spring的热更实现)
//     *
//     * @param changedClass 新Class
//     */
//    void reload(Class<?> changedClass, DynamicClassLoader classLoader) {
//        Class<?> my = this.getClass();
//        //不断往父类遍历,替换受影响的Field
//        for (Field field : ClassUtils.getAllFields(my)) {
//            try {
//                field.setAccessible(true);
//                if (field.get(this) != null
//                        && field.getType().isAssignableFrom(changedClass)
//                        && field.get(this).getClass().getName().equals(changedClass.getName())) {
//                    //实现类相同
//                    Object newObj = changedClass.newInstance();
//                    field.set(this, newObj);
//                }
//            } catch (IllegalAccessException | InstantiationException e) {
//                ExceptionUtils.log(e);
//            } finally {
//                field.setAccessible(false);
//            }
//        }
//    }
//}
