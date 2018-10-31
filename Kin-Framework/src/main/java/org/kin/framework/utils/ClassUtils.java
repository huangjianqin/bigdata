package org.kin.framework.utils;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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

/**
 * Created by huangjianqin on 2018/1/26.
 */
public class ClassUtils {
    public static final String CLASS_SUFFIX = ".class";

    /**
     * 通过无参构造器实例化类
     */
    public static <T> T instance(Class<T> claxx){
        if(claxx == null){
            return null;
        }
        try {
            return claxx.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            ExceptionUtils.log(e);
        }
        return null;
    }

    public static <T> T instance(String classStr){
        if(classStr == null || classStr.equals("")){
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
    public static <T> T instance(Class<T> claxx, Object... args){
        if(claxx == null){
            return null;
        }
        try {
            List<Class> argClasses = new ArrayList<>();
            for(Object arg: args){
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

    public static Class getClass(String className){
        if(className == null || className.equals("")){
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
                if (url.getProtocol().equals("file")) {
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
                } else if (url.getProtocol().equals("jar") && isIncludeJar) {
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
        for(Field field: getAllFields(target.getClass())){
            if(field.getName().equals(fieldName)){
                field.setAccessible(true);
                try {
                    return (T)field.get(target);
                } catch (IllegalAccessException e) {
                    ExceptionUtils.log(e);
                }
                finally {
                    field.setAccessible(false);
                }
            }
        }

        return null;
    }

    public static void setFieldValue(Object target, String fieldName, Object newValue) {
        for(Field field: getAllFields(target.getClass())){
            if(field.getName().equals(fieldName)){
                field.setAccessible(true);
                try {
                    field.set(target, newValue);
                } catch (IllegalAccessException e) {
                    ExceptionUtils.log(e);
                }
                finally {
                    field.setAccessible(false);
                }
            }
        }
    }

    public static void setFieldValue(Object target, Field field, Object newValue) {
        Set<Field> fields = getAllFields(target.getClass());
        if(fields.contains(field)){
            try {
                field.set(target, newValue);
            } catch (IllegalAccessException e) {
                ExceptionUtils.log(e);
            }
        }
    }

    public static Set<Field> getAllFields(Class<?> claxx){
        return getFields(claxx, Object.class);
    }

    /**
     * 获取claxx -> parent的所有field
     */
    public static Set<Field> getFields(Class<?> claxx, Class<?> parent){
        if(parent.isAssignableFrom(claxx)){
            throw new IllegalStateException(String.format("%s is not super class of %s", parent.getName(), claxx.getName()));
        }
        Set<Field> fields = new HashSet<>();
        while(!claxx.equals(parent)){
            for(Field field: claxx.getDeclaredFields()){
                fields.add(field);
            }
            claxx = claxx.getSuperclass();
        }
        return fields;
    }

    public static Set<Class<?>> getAllClasses(Class<?> claxx){
        return getClasses(claxx, Object.class);
    }

    /**
     * 获取claxx -> parent的所有class
     */
    public static Set<Class<?>> getClasses(Class<?> claxx, Class<?> parent){
        if(parent.isAssignableFrom(claxx)){
            throw new IllegalStateException(String.format("%s is not super class of %s", parent.getName(), claxx.getName()));
        }
        Set<Class<?>> classes = new HashSet<>();
        while(!claxx.equals(parent)){
            classes.add(claxx);
            claxx = claxx.getSuperclass();
        }
        return classes;
    }
}
