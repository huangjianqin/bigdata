package org.kin.framework.utils;

import com.google.common.collect.Sets;
import scala.annotation.meta.field;
import sun.jvm.hotspot.runtime.Bytes;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.checkerframework.checker.units.UnitsTools.m;

/**
 * Created by huangjianqin on 2018/1/26.
 */
public class ClassUtils {
    public static final String CLASS_SUFFIX = ".class";
    //基础类型类名
    public static final String STRING_CLASS = "class java.lang.String";
    public static final String CHAR = "char";
    public static final String INTEGER_CLASS = "class java.lang.Integer";
    public static final String INT = "int";
    public static final String DOUBLE_CLASS = "class java.lang.Double";
    public static final String DOUBLE = "double";
    public static final String LONG_CLASS = "class java.lang.Long";
    public static final String LONG = "long";
    public static final String BYTE_CLASS = "class java.lang.Byte";
    public static final String BYTE = "byte";
    public static final String SHORT_CLASS = "class java.lang.Short";
    public static final String SHORT = "short";
    public static final String FLOAT_CLASS = "class java.lang.Float";
    public static final String FLOAT = "float";

    /**
     * 通过无参构造器实例化类
     */
    public static <T> T instance(Class<T> claxx) {
        if (claxx == null) {
            return null;
        }
        try {
            return claxx.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            ExceptionUtils.log(e);
        }
        return null;
    }

    public static <T> T instance(String classStr) {
        if (StringUtils.isBlank(classStr)) {
            return null;
        }
        try {
            Class<T> claxx = (Class<T>) Class.forName(classStr);
            return instance(claxx);
        } catch (ClassNotFoundException e) {
            ExceptionUtils.log(e);
        }
        return null;
    }

    /**
     * 根据参数调用构造器实例化类
     */
    public static <T> T instance(Class<T> claxx, Object... args) {
        if (claxx == null) {
            return null;
        }
        try {
            List<Class> argClasses = new ArrayList<>();
            for (Object arg : args) {
                argClasses.add(arg.getClass());
            }
            Constructor<T> constructor = claxx.getDeclaredConstructor(argClasses.toArray(new Class[1]));
            T target = constructor.newInstance(args);
            return target;
        } catch (InstantiationException | IllegalAccessException |
                NoSuchMethodException | InvocationTargetException e) {
            ExceptionUtils.log(e);
        }
        return null;
    }

    public static Class getClass(String className) {
        if (StringUtils.isBlank(className)) {
            return null;
        }
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            ExceptionUtils.log(e);
        }
        return null;
    }

    public static <T> Set<Class<T>> getSubClass(String packageName, Class<T> parent, boolean isIncludeJar) {
        Set<Class<T>> subClasses = Sets.newLinkedHashSet();
        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

        String packageResource = packageName.replaceAll("\\.", "/");
        try {
            Enumeration<URL> urls = currentClassLoader.getResources(packageResource);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if ("file".equals(url.getProtocol())) {
                    Path path = Paths.get(url.toURI());
                    Stream<Path> stream = Files.walk(path);
                    Set<Class<T>> classes = stream.filter(p -> !Files.isDirectory(p) && p.toString().endsWith(CLASS_SUFFIX))
                            .map(p -> {
                                URI uri = p.toUri();
                                String origin = uri.toString().replaceAll("/", ".");
                                int startIndex = origin.indexOf(packageName);
                                int endIndex = origin.lastIndexOf(CLASS_SUFFIX);

                                String className = origin.substring(startIndex, endIndex);
                                try {
                                    return (Class<T>) currentClassLoader.loadClass(className);
                                } catch (ClassNotFoundException e) {
                                    ExceptionUtils.log(e);
                                }
                                return null;
                            })
                            .filter(claxx -> claxx != null
                                    && !parent.equals(claxx)
                                    && parent.isAssignableFrom(claxx)
                            ).collect(Collectors.toSet());
                    subClasses.addAll(Sets.newHashSet(classes));
                } else if ("jar".equals(url.getProtocol()) && isIncludeJar) {
                    JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
                    JarFile jarFile = jarURLConnection.getJarFile();
                    Enumeration<JarEntry> jarEntries = jarFile.entries();
                    while (jarEntries.hasMoreElements()) {
                        JarEntry jarEntry = jarEntries.nextElement();
                        String entryName = jarEntry.getName();

                        if (entryName.endsWith("/") || !entryName.endsWith(CLASS_SUFFIX)) {
                            continue;
                        }

                        String className = entryName.replaceAll("/", ".");
                        try {
                            Class<T> claxx = (Class<T>) currentClassLoader.loadClass(className);
                            subClasses.add(claxx);
                        } catch (ClassNotFoundException e) {
                            ExceptionUtils.log(e);
                        }
                    }
                }
            }
        } catch (IOException | URISyntaxException e) {
            ExceptionUtils.log(e);
        }

        return subClasses;
    }

    /**
     * 从子类往父类遍历,获取成员变量实例
     */
    public static <T> T getFieldValue(Object target, String fieldName) {
        for (Field field : getAllFields(target.getClass())) {
            if (field.getName().equals(fieldName)) {
                field.setAccessible(true);
                try {
                    return (T) field.get(target);
                } catch (IllegalAccessException e) {
                    ExceptionUtils.log(e);
                } finally {
                    field.setAccessible(false);
                }
            }
        }

        return null;
    }

    /**
     * 通过setter field实例设置值
     */
    public static void setFieldValue(Object instance, Field field, Object value) {
        if (value == null) {
            return;
        }
        try {
            Method m = setterMethod(instance, field.getName(), value);
            if (m != null) {
                m.invoke(instance, value);
            } else {
                try{
                    field.setAccessible(true);
                    field.set(instance, value);
                }
                finally {
                    field.setAccessible(false);
                }
            }

        } catch (Exception e) {
            ExceptionUtils.log(e);
        }
    }

    public static void setFieldValue(Object target, String fieldName, Object newValue) {
        for (Field field : getAllFields(target.getClass())) {
            if (field.getName().equals(fieldName)) {
                field.setAccessible(true);
                try {
                    field.set(target, newValue);
                } catch (IllegalAccessException e) {
                    ExceptionUtils.log(e);
                } finally {
                    field.setAccessible(false);
                }
            }
        }
    }

    public static Method getterMethod(Object instance, String fieldName) throws Exception {
        byte[] items = fieldName.getBytes();
        items[0] = (byte) ((char) items[0] - 'a' + 'A');
        return instance.getClass().getMethod("get" + new String(items));
    }

    public static Method setterMethod(Object instance, String fieldName, Object value) throws Exception {
        byte[] items = fieldName.getBytes();
        items[0] = (byte) ((char) items[0] - 'a' + 'A');
        return instance.getClass().getMethod("set" + new String(items), value.getClass());
    }

    public static Set<Field> getAllFields(Class<?> claxx) {
        return getFields(claxx, Object.class);
    }

    /**
     * 获取claxx -> parent的所有field
     */
    public static Set<Field> getFields(Class<?> claxx, Class<?> parent) {
        if (parent.isAssignableFrom(claxx)) {
            throw new IllegalStateException(String.format("%s is not super class of %s", parent.getName(), claxx.getName()));
        }
        Set<Field> fields = new HashSet<>();
        while (!claxx.equals(parent)) {
            for (Field field : claxx.getDeclaredFields()) {
                fields.add(field);
            }
            claxx = claxx.getSuperclass();
        }
        return fields;
    }

    public static Set<Class<?>> getAllClasses(Class<?> claxx) {
        return getClasses(claxx, Object.class);
    }

    /**
     * 获取claxx -> parent的所有class
     */
    public static Set<Class<?>> getClasses(Class<?> claxx, Class<?> parent) {
        if (parent.isAssignableFrom(claxx)) {
            throw new IllegalStateException(String.format("%s is not super class of %s", parent.getName(), claxx.getName()));
        }
        Set<Class<?>> classes = new HashSet<>();
        while (!claxx.equals(parent)) {
            classes.add(claxx);
            claxx = claxx.getSuperclass();
        }
        return classes;
    }

    /**
     * 获取默认值
     */
    public static Object getDefaultValue(Class claxx){
        if(claxx.isPrimitive()){
            if (ClassUtils.CHAR.equals(claxx.toString())) {
                return "";
            }

            if (ClassUtils.INTEGER_CLASS.equals(claxx.toString()) ||
                    ClassUtils.INT.equals(claxx.toString())) {
                return 0;
            }

            if (ClassUtils.DOUBLE_CLASS.equals(claxx.toString()) ||
                    ClassUtils.DOUBLE.equals(claxx.toString())) {
                return 0D;
            }

            if (ClassUtils.LONG_CLASS.equals(claxx.toString()) ||
                    ClassUtils.LONG.equals(claxx.toString())) {
                return 0L;
            }

            if (ClassUtils.BYTE_CLASS.equals(claxx.toString()) ||
                    ClassUtils.BYTE.equals(claxx.toString())) {
                return 0;
            }

            if (ClassUtils.SHORT_CLASS.equals(claxx.toString()) ||
                    ClassUtils.SHORT.equals(claxx.toString())) {
                return 0;
            }

            if (ClassUtils.FLOAT_CLASS.equals(claxx.toString()) ||
                    ClassUtils.FLOAT.equals(claxx.toString())) {
                return 0F;
            }
        }
        return null;
    }
}
