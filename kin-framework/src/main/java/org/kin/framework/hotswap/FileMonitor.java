package org.kin.framework.hotswap;

import org.kin.framework.Closeable;
import org.kin.framework.JvmCloseCleaner;
import org.kin.framework.concurrent.SimpleThreadFactory;
import org.kin.framework.concurrent.ThreadManager;
import org.kin.framework.hotswap.agent.JavaAgentHotswap;
import org.kin.framework.utils.ClassUtils;
import org.kin.framework.utils.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by huangjianqin on 2018/2/1.
 * 文件监听器   单例模式
 * 利用nio 新api监听文件变换
 * 该api底层本质上是监听了操作系统的文件系统触发的文件更改事件
 * <p>
 * 异步热加载文件 同步类热更新
 */
public class FileMonitor extends Thread implements Closeable{
    private static final Logger log = LoggerFactory.getLogger("hotSwap");
    //默认实现
    private static final FileMonitor COMMON = new FileMonitor();
    private static volatile boolean isStarted = false;

    static {
        JvmCloseCleaner.DEFAULT().add(() -> {
            COMMON.shutdown();
        });
    }

    private WatchService watchService;
    //hash(file name) -> Reloadable 实例
    private Map<Integer, AbstractFileReloadable> monitorItems;
    //类热加载工厂
//    private AbstractHotswapFactory hotswapFactory;
    private JavaAgentHotswap javaAgentHotswap = JavaAgentHotswap.instance();
    //分段锁
    private Object[] locks;
    //异步热加载文件 执行线程
    private ThreadManager threadManager;
    private volatile boolean isStopped = false;

    private FileMonitor() {

    }

    public static FileMonitor instance() {
        if (!isStarted) {
            COMMON.start();
        }
        return COMMON;
    }

    private void init() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }

        monitorItems = new HashMap<>();
        locks = new Object[5];
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
//        if (hotswapFactory == null) {
//            //默认设置
//            hotswapFactory = new CommonHotswapFactory();
//        }
        if (threadManager == null) {
            //默认设置
            ExecutorService executorService = new ThreadPoolExecutor(0, 5, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(), new SimpleThreadFactory("file-monitor"));
            this.threadManager = new ThreadManager(executorService);
        }

//        if (hotswapFactory instanceof CommonHotswapFactory || hotswapFactory instanceof JavaAgentHotswap) {
//            monitorClasspath();
//        }
        //监听热更class存储目录
        Path classesPath = Paths.get(JavaAgentHotswap.getClasspath());
        try {
            classesPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }

        monitorJVMClose();
    }

//    public FileMonitor hotswapFactory(AbstractHotswapFactory hotswapFactory) {
//        this.hotswapFactory = hotswapFactory;
//        return this;
//    }

    /**
     * 监听整个classpath
     */
//    private void monitorClasspath() {
//        String classRoot = Thread.currentThread().getContextClassLoader().getResource("").getPath();
//        Path classRootPath = Paths.get(classRoot);
//        try {
//            Stream<Path> allDir = Files.walk(classRootPath).filter(p -> Files.isDirectory(p));
//            allDir.forEach(p -> {
//                try {
//                    p.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
//                } catch (IOException e) {
//                    ExceptionUtils.log(e);
//                }
//            });
//        } catch (IOException e) {
//            ExceptionUtils.log(e);
//        }
//    }



    @Override
    public synchronized void start() {
        init();
        super.start();
    }

    @Override
    public void run() {
        log.info("file monitor start");
        while (!isStopped && !Thread.currentThread().isInterrupted()) {
            List<Path> changedClasses = new ArrayList<>();
            try {
                WatchKey key = watchService.take();
                //变化的路径
                Path parentPath = (Path) key.watchable();
                List<WatchEvent<?>> events = key.pollEvents();
                events.forEach(event -> {
                    //变化item的名字(文件名或者文件夹名)
                    String itemName = event.context().toString();
                    int hashKey = itemName.hashCode();
                    //真实路径
                    Path childPath = Paths.get(parentPath.toString(), itemName);
                    log.debug("'{}' changed", childPath.toString());
                    if (!Files.isDirectory(childPath)) {
                        //非文件夹
                        if (itemName.endsWith(ClassUtils.CLASS_SUFFIX)) {
                            //处理类热更新
                            changedClasses.add(childPath);
                        } else {
                            synchronized (getLock(hashKey)) {
                                //处理文件热更新
                                AbstractFileReloadable fileReloadable = monitorItems.get(hashKey);
                                if (fileReloadable != null) {
                                    threadManager.execute(() -> {
                                        try {
                                            long startTime = System.currentTimeMillis();
                                            try (InputStream is = new FileInputStream(childPath.toFile())) {
                                                fileReloadable.reload(is);
                                            }
                                            long endTime = System.currentTimeMillis();
                                            log.info("hotswap file '{}' finished, time cost {} ms", childPath.toString(), endTime - startTime);
                                        } catch (IOException e) {
                                            ExceptionUtils.log(e);
                                        }
                                    });
                                }
                            }
                        }
                    }
                });
                //重置状态，让key等待事件
                key.reset();
            } catch (InterruptedException e) {
                ExceptionUtils.log(e);
            }

            if (changedClasses.size() > 0) {
                //类热更新
//                threadManager.execute(() -> hotswapFactory.reload(changedClasses));
                threadManager.execute(() -> javaAgentHotswap.hotswap(changedClasses));
                HotFix.instance().fix();
            }
        }
        log.info("file monitor end");
    }

    /**
     * 获取分段锁
     */
    private Object getLock(int key) {
        return locks[key % locks.length];
    }

    public void shutdown() {
        if (!isStopped) {
            isStopped = true;
            try {
                watchService.close();
            } catch (IOException e) {
                ExceptionUtils.log(e);
            }
            threadManager.shutdown();
            //help GC
            monitorItems = null;
//            hotswapFactory = null;
            locks = null;
            threadManager = null;

            //中断监控线程, 让本线程退出
            interrupt();
        }
    }

    //-----------------------------------------------------------------------------------------------------------------
    private void checkStatus() {
        if (isStopped) {
            throw new IllegalStateException("file monitor has shutdowned");
        }
    }

//    /**
//     * @param classReloadable 监听该实例
//     */
//    public void monitorObject(ClassReloadableEntity classReloadable) {
//        checkStatus();
//        if (hotswapFactory instanceof CommonHotswapFactory) {
//            ((CommonHotswapFactory) hotswapFactory).register(classReloadable);
//        }
//    }

    public void monitorFile(String pathStr, AbstractFileReloadable fileReloadable) {
        checkStatus();
        Path path = Paths.get(pathStr);
        monitorFile(path, fileReloadable);
    }

    public void monitorFile(Path path, AbstractFileReloadable fileReloadable) {
        checkStatus();
        if (!Files.isDirectory(path)) {
            monitorFile0(path.getParent(), path.getFileName().toString(), fileReloadable);
        } else {
            throw new IllegalStateException("monitor file dir error");
        }
    }

    /**
     * 监听文件变化
     */
    private void monitorFile0(Path file, String itemName, AbstractFileReloadable fileReloadable) {
        try {
            file.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
        } catch (IOException e) {
            ExceptionUtils.log(e);
        }

        int key = itemName.hashCode();
        synchronized (getLock(key)) {
            monitorItems.put(key, fileReloadable);
        }
    }

    @Override
    public void close() {
        shutdown();
    }
}
