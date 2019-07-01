package org.kin.framework.hotswap.agent;

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.classfile.ClassFile;
import com.sun.tools.classfile.ConstantPoolException;
import org.kin.framework.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.instrument.ClassDefinition;
import java.lang.instrument.UnmodifiableClassException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by huangjianqin on 2018/2/3.
 * 单例模式
 */
public final class JavaAgentHotswap implements JavaAgentHotswapMBean{
    private static final Logger log = LoggerFactory.getLogger("hotSwap");
    private static final String JAR_SUFFIX = ".jar";
    //热更class文件放另外一个目录
    //开发者指定, 也可以走配置
    private static final String CLASSPATH = "hotswap/classes";
    private static final String JAR_PATH = "hotswap/KinJavaAgent.jar";
    private volatile boolean isInit;
    private volatile Map<String, ClassFileInfo> filePath2ClassFileInfo = new HashMap<>();

    static {
//        CLASSPATH = JavaAgentHotswap.class.getClassLoader().getResource("").getPath();
        log.info("java agent:classpath:{}", CLASSPATH);

//        JAR_PATH = getJarPath();
        log.info("java agent:jarPath:{}", JAR_PATH);
    }

    private static final JavaAgentHotswap HOTSWAP_FACTORY = new JavaAgentHotswap();

    public static JavaAgentHotswap instance() {
        if (!HOTSWAP_FACTORY.isInit) {
            HOTSWAP_FACTORY.init();
            HOTSWAP_FACTORY.isInit = true;
        }
        return HOTSWAP_FACTORY;
    }

    private JavaAgentHotswap() {
    }

    public static String getClasspath() {
        return CLASSPATH;
    }

    private void init() {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(this + ":type=JavaAgentHotswap");
            mBeanServer.registerMBean(this, name);
        } catch (MalformedObjectNameException | NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            ExceptionUtils.log(e);
        }
    }

    /**
     * 获取jar包路径
     */
    private static String getJarPath() {
        //JavaDynamicAgent是jar文件内容,也就是说jar必须包含JavaDynamicAgent
        URL url = JavaDynamicAgent.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = null;
        try {
            // 转化为utf-8编码
            filePath = URLDecoder.decode(url.getPath(), "utf-8");
        } catch (Exception e) {
            ExceptionUtils.log(e);
        }
        // 可执行jar包运行的结果里包含".jar"
        if (filePath.endsWith(JAR_SUFFIX)) {
            // 截取路径中的jar包名
            filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        }

        File file = new File(filePath);

        filePath = file.getAbsolutePath();
        return filePath;
    }

    public synchronized void hotswap(List<Path> changedPaths) {
        long startTime = System.currentTimeMillis();
        log.info("开始热更类...");
        try {
            List<ClassDefinition> classDefList = new ArrayList<>();
            for (Path changedPath : changedPaths) {
                String classFilePath = changedPath.toString();
                ClassFileInfo old = filePath2ClassFileInfo.get(classFilePath);
                //过滤没有变化的文件(通过文件修改时间)
                long classFileLastModifiedTime = Files.getLastModifiedTime(changedPath).toMillis();
                if (old == null || old.getLastModifyTime() != classFileLastModifiedTime) {
                    log.info("开始检查文件'{}'", classFilePath);
                    boolean checkClassName = false;
                    byte[] bytes = Files.readAllBytes(changedPath);

                    //从class文件字节码中读取className
                    String className = null;
                    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
                        ClassFile cf = ClassFile.read(dis);
                        className = cf.getName().replaceAll("/", "\\.");
                    } catch (IOException | ConstantPoolException e) {
                        ExceptionUtils.log(e);
                    }

                    ClassFileInfo cfi = new ClassFileInfo(classFilePath, className, bytes, classFileLastModifiedTime);
                    //检查类名
                    if (old == null || old.getClassName().equals(cfi.getClassName())) {
                        log.info("文件'{}' 类'{}'检查成功", classFilePath, className);
                        checkClassName = true;
                        filePath2ClassFileInfo.put(classFilePath, cfi);
                    }

                    if (checkClassName) {
                        Class<?> c = Class.forName(className);
                        ClassDefinition classDefinition = new ClassDefinition(c, bytes);

                        classDefList.add(classDefinition);
                    } else {
                        throw new IllegalStateException("因为文件 '" + classFilePath + "' 解析失败, 故热更失败");
                    }
                }
            }


            // 当前进程pid
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String pid = name.split("@")[0];
            log.debug("当前进程pid：{}", pid);

            // 虚拟机加载
            VirtualMachine vm = null;
            try {
                vm = VirtualMachine.attach(pid);
                //JavaDynamicAgent所在的jar包
                //app jar包与agent jar包同一路径
                vm.loadAgent(JAR_PATH);

                //重新定义类
                JavaDynamicAgent.getInstrumentation().redefineClasses(classDefList.toArray(new ClassDefinition[classDefList.size()]));

                //删除热更类文件
                Path rootPath = Paths.get(CLASSPATH);
                Files.list(rootPath).forEach(childpath -> {
                    try {
                        Files.deleteIfExists(childpath);
                    } catch (IOException e) {
                        ExceptionUtils.log(e);
                    }
                });
            } catch (AttachNotSupportedException | AgentLoadException | AgentInitializationException e) {
                ExceptionUtils.log(e);
            } catch (IOException e) {
                ExceptionUtils.log(e);
            } finally {
                if (vm != null) {
                    try {
                        vm.detach();
                    } catch (IOException e) {
                        ExceptionUtils.log(e);
                    }
                }
            }
        } catch (IOException | UnmodifiableClassException | ClassNotFoundException e) {
            ExceptionUtils.log(e);
        } finally {
            long endTime = System.currentTimeMillis();
            log.info("...热更类结束, 耗时 {} ms", endTime - startTime);
        }
    }

    @Override
    public List<ClassFileInfo> getClassFileInfo() {
        List<ClassFileInfo> classFileInfos = new ArrayList<>(filePath2ClassFileInfo.values());
        return classFileInfos;
    }
}
