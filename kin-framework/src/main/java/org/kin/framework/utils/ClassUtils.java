package org.kin.framework.utils;

import com.google.common.collect.Sets;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by huangjianqin on 2018/1/26.
 */
public class ClassUtils {
    public static final String CLASS_SUFFIX = ".class";
    //用于匹配内部类
    private static final Pattern INNER_PATTERN = Pattern.compile("\\$(\\d+).", Pattern.CASE_INSENSITIVE);

    @FunctionalInterface
    private interface Matcher<T> {
        /**
         * @param c      基准
         * @param target 目标
         * @return true表示匹配
         */
        boolean match(Class<T> c, Class<T> target);
    }

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

    /**
     * 获取某个类的所有子类, 但不包括该类
     */
    public static <T> Set<Class<T>> getSubClass(String packageName, Class<T> parent, boolean isIncludeJar) {
        return scanClasspathAndFindMatch(packageName, parent,
                (c, target) -> !c.equals(target) && c.isAssignableFrom(target), isIncludeJar);
    }

    /**
     * 获取出现某注解的所有类, 包括抽象类和接口
     */
    public static <T> Set<Class<T>> getAnnotationedClass(String packageName, Class<T> annotationClass, boolean isIncludeJar) {
        if (annotationClass.isAnnotation()) {
            return scanClasspathAndFindMatch(packageName, annotationClass,
                    (c, target) -> {
                        if (target.isAnnotationPresent(c)) {
                            return true;
                        } else {
                            for (Field field : target.getDeclaredFields()) {
                                if (field.isAnnotationPresent(c)) {
                                    return true;
                                }
                            }

                            for (Method method : target.getDeclaredMethods()) {
                                if (method.isAnnotationPresent(c)) {
                                    return true;
                                }
                            }
                        }

                        return false;
                    }, isIncludeJar);
        }
        return Collections.emptySet();
    }

    public static <T> Set<Class<T>> scanClasspathAndFindMatch(String packageName, Class<T> c, Matcher matcher, boolean isIncludeJar) {
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
                                if (StringUtils.isNotBlank(className) &&
                                        !INNER_PATTERN.matcher(className).find() &&
                                        !(className.indexOf("$") > 0)) {
                                    try {
                                        return (Class<T>) currentClassLoader.loadClass(className);
                                    } catch (ClassNotFoundException e) {

                                    }
                                }
                                return null;
                            })
                            .filter(claxx -> !Objects.isNull(claxx))
                            .filter(claxx -> matcher.match(c, claxx)).collect(Collectors.toSet());
                    subClasses.addAll(Sets.newHashSet(classes));
                } else if ("jar".equals(url.getProtocol()) && isIncludeJar) {
                    JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
                    JarFile jarFile = jarURLConnection.getJarFile();
                    Enumeration<JarEntry> jarEntries = jarFile.entries();
                    while (jarEntries.hasMoreElements()) {
                        JarEntry jarEntry = jarEntries.nextElement();
                        String entryName = jarEntry.getName();

                        if (jarEntry.isDirectory()) {
                            continue;
                        }

                        if (entryName.endsWith("/") || !entryName.endsWith(CLASS_SUFFIX)) {
                            continue;
                        }

                        if (INNER_PATTERN.matcher(entryName).find() || entryName.indexOf("$") > 0) {
                            continue;
                        }

                        String className = entryName.replaceAll("/", ".");
                        try {
                            Class<T> claxx = (Class<T>) currentClassLoader.loadClass(className);
                            if (matcher.match(c, claxx)) {
                                subClasses.add(claxx);
                            }
                        } catch (ClassNotFoundException e) {

                        }
                    }
                }
            }
        } catch (IOException | URISyntaxException e) {

        }

        return subClasses;
    }

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

    /**
     * 通过getter field实例设置值
     */
    public static Object getFieldValue(Object instance, Field field) {
        try {
            Method m = getterMethod(instance, field.getName());
            if (m != null) {
                return m.invoke(instance);
            } else {
                try {
                    field.setAccessible(true);
                    return field.get(instance);
                } finally {
                    field.setAccessible(false);
                }
            }

        } catch (Exception e) {
            ExceptionUtils.log(e);
        }

        return getDefaultValue(field.getType());
    }

    /**
     * 通过setter field实例设置值
     */
    public static void setFieldValue(Object instance, Field field, Object value) {
        try {
            Method m = setterMethod(instance, field.getName(), value);
            if (m != null) {
                m.invoke(instance, value);
            } else {
                try {
                    field.setAccessible(true);
                    field.set(instance, value);
                } finally {
                    field.setAccessible(false);
                }
            }
        } catch (Exception e) {
            ExceptionUtils.log(e);
        }
    }

    public static Method getterMethod(Object instance, String fieldName) {
        byte[] items = fieldName.getBytes();
        items[0] = (byte) ((char) items[0] - 'a' + 'A');
        try {
            return instance.getClass().getMethod("get" + new String(items));
        } catch (NoSuchMethodException e) {

        }

        return null;
    }

    public static Method setterMethod(Object instance, String fieldName, Object value) {
        byte[] items = fieldName.getBytes();
        items[0] = (byte) ((char) items[0] - 'a' + 'A');
        try {
            return instance.getClass().getMethod("set" + new String(items), value.getClass());
        } catch (NoSuchMethodException e) {

        }

        return null;
    }

    public static List<Field> getAllFields(Class<?> claxx) {
        return getFields(claxx, Object.class);
    }

    /**
     * 获取claxx -> parent的所有field
     */
    public static List<Field> getFields(Class<?> claxx, Class<?> parent) {
        if (claxx == null || parent == null) {
            return Collections.emptyList();
        }

        List<Field> fields = new ArrayList<>();
        while (!claxx.equals(parent)) {
            for (Field field : claxx.getDeclaredFields()) {
                fields.add(field);
            }
            claxx = claxx.getSuperclass();
        }
        return fields;
    }

    public static List<Class<?>> getAllClasses(Class<?> claxx) {
        return getClasses(claxx, Object.class);
    }

    /**
     * 获取claxx -> parent的所有class
     */
    public static List<Class<?>> getClasses(Class<?> claxx, Class<?> parent) {
        if (parent.isAssignableFrom(claxx)) {
            throw new IllegalStateException(String.format("%s is not super class of %s", parent.getName(), claxx.getName()));
        }
        List<Class<?>> classes = new ArrayList<>();
        while (!claxx.equals(parent)) {
            classes.add(claxx);
            claxx = claxx.getSuperclass();
        }
        return classes;
    }

    /**
     * 获取默认值
     */
    public static Object getDefaultValue(Class claxx) {
        if (claxx.isPrimitive()) {
            if (String.class.equals(claxx)) {
                return "";
            } else if (Boolean.class.equals(claxx) || Boolean.TYPE.equals(claxx)) {
                return false;
            } else if (Byte.class.equals(claxx) || Byte.TYPE.equals(claxx)) {
                return 0;
            } else if (Character.class.equals(claxx) || Character.TYPE.equals(claxx)) {
                return "";
            } else if (Short.class.equals(claxx) || Short.TYPE.equals(claxx)) {
                return 0;
            } else if (Integer.class.equals(claxx) || Integer.TYPE.equals(claxx)) {
                return 0;
            } else if (Long.class.equals(claxx) || Long.TYPE.equals(claxx)) {
                return 0L;
            } else if (Float.class.equals(claxx) || Float.TYPE.equals(claxx)) {
                return 0.0F;
            } else if (Double.class.equals(claxx) || Double.TYPE.equals(claxx)) {
                return 0.0D;
            }
        }
        return null;
    }

    public static <T> T convertBytes2PrimitiveObj(Class<T> claxx, byte[] bytes) {
        if (bytes == null || bytes.length <= 0) {
            return null;
        }

        String strValue = new String(bytes);
        return convertStr2PrimitiveObj(claxx, strValue);
    }

    public static <T> T convertStr2PrimitiveObj(Class<T> claxx, String strValue) {
        if (StringUtils.isNotBlank(strValue)) {
            if (String.class.equals(claxx)) {
                return (T) strValue;
            } else if (Boolean.class.equals(claxx) || Boolean.TYPE.equals(claxx)) {
                return (T) Boolean.valueOf(strValue);
            } else if (Byte.class.equals(claxx) || Byte.TYPE.equals(claxx)) {
                return (T) Byte.valueOf(strValue);
            } else if (Character.class.equals(claxx) || Character.TYPE.equals(claxx)) {
                return (T) strValue;
            } else if (Short.class.equals(claxx) || Short.TYPE.equals(claxx)) {
                return (T) Short.valueOf(strValue);
            } else if (Integer.class.equals(claxx) || Integer.TYPE.equals(claxx)) {
                return (T) Integer.valueOf(strValue);
            } else if (Long.class.equals(claxx) || Long.TYPE.equals(claxx)) {
                return (T) Long.valueOf(strValue);
            } else if (Float.class.equals(claxx) || Float.TYPE.equals(claxx)) {
                return (T) Float.valueOf(strValue);
            } else if (Double.class.equals(claxx) || Double.TYPE.equals(claxx)) {
                return (T) Double.valueOf(strValue);
            }
        }

        return null;
    }
}
