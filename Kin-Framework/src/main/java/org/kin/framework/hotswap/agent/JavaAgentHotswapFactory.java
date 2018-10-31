package org.kin.framework.hotswap.agent;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import org.kin.framework.hotswap.HotswapFactory;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.lang.instrument.ClassDefinition;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huangjianqin on 2018/2/3.
 * 单例模式
 */
public class JavaAgentHotswapFactory extends HotswapFactory {
    private static final Logger log = LoggerFactory.getLogger("hot-fix-class");
    private static final String classesPath;
    private static final String jarPath;
    private static VirtualMachine vm;
    private static final String pid;

    private boolean isInited = false;

    static {
        classesPath = JavaAgentHotswapFactory.class.getClassLoader().getResource("").getPath();
        log.info("java agent:classpath:{}", classesPath);

        jarPath = getJarPath();
        log.info("java agent:jarPath:{}", jarPath);

        // 当前进程pid
        String name = ManagementFactory.getRuntimeMXBean().getName();
        pid = name.split("@")[0];
        log.info("当前进程pid：{}", pid);

        // 虚拟机加载
        try {
            vm = VirtualMachine.attach(pid);
            //JavaDynamicAgent所在的jar包
            //app jar包与agent jar包同一路径
            vm.loadAgent(jarPath + "/JavaDynamicAgent.jar");
        } catch (AttachNotSupportedException | AgentLoadException | AgentInitializationException e) {
            ExceptionUtils.log(e);
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }
    }

    private static final JavaAgentHotswapFactory hotswapFactory = new JavaAgentHotswapFactory();

    public static JavaAgentHotswapFactory instance(){
        return hotswapFactory;
    }

    private JavaAgentHotswapFactory() {
    }

    /**
     * 获取jar包路径
     */
    private static String getJarPath() {
        //JavaDynamicAgent是jar文件内容,也就是说jar必须包含JavaDynamicAgent
        URL url = JavaDynamicAgent.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = null;
        try {
            filePath = URLDecoder.decode(url.getPath(), "utf-8");// 转化为utf-8编码
        } catch (Exception e) {
            ExceptionUtils.log(e);
        }
        if (filePath.endsWith(".jar")) {// 可执行jar包运行的结果里包含".jar"
            // 截取路径中的jar包名
            filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        }

        File file = new File(filePath);

        filePath = file.getAbsolutePath();
        return filePath;
    }

    private void init(){
        //获取Instrumentation
        Instrumentation instrumentation = JavaDynamicAgent.getInstrumentation();
        Preconditions.checkNotNull(instrumentation, "initInstrumentation must not be null");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            destroy();
        }));
    }

    private void destroy(){
        if (vm != null) {
            try {
                vm.detach();
            } catch (IOException e) {
                ExceptionUtils.log(e);
            }
        }
    }

    @Override
    public void reload(List<Path> changedPaths) {
        if(!isInited){
            init();
            isInited = true;
        }

        try {
            //重新加载
            List<ClassDefinition> classDefList = new ArrayList<>();
            for (Path changedPath: changedPaths) {
                String classPath = changedPath.toString();
                String tmp = classPath.replaceAll("/", "\\.");

                String className = tmp.substring(0, tmp.indexOf(ClassUtils.CLASS_SUFFIX));
                Class<?> c = Class.forName(className);

                log.info("class redefined:" + classPath);
                byte[] bytesFromFile = Files.toByteArray(new File(classPath));
                ClassDefinition classDefinition = new ClassDefinition(c, bytesFromFile);

                classDefList.add(classDefinition);
            }
            //重新定义类
            JavaDynamicAgent.getInstrumentation().redefineClasses(classDefList.toArray(new ClassDefinition[classDefList.size()]));
        } catch (IOException | UnmodifiableClassException | ClassNotFoundException e) {
            ExceptionUtils.log(e);
        }
    }
}
