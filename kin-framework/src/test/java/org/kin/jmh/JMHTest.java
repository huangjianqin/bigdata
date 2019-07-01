package org.kin.jmh; /**
 * @author huangjianqin
 * @date 2019/7/1
 */

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)//基准测试类型
@OutputTimeUnit(TimeUnit.SECONDS)//基准测试结果的时间类型
@Warmup(iterations = 1)//预热的迭代次数
@Threads(2)//测试线程数量
@State(Scope.Thread)//该状态为每个线程独享
//度量:iterations进行测试的轮次，time每轮进行的时长，timeUnit时长单位,batchSize批次数量
@Measurement(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS, batchSize = 5)
public class JMHTest {
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JMHTest.class.getSimpleName())
                .forks(1)
                //     使用之前要安装hsdis
                //-XX:-TieredCompilation 关闭分层优化 -server
                //-XX:+LogCompilation  运行之后项目路径会出现按照测试顺序输出hotspot_pid<PID>.log文件,可以使用JITWatch进行分析,可以根据最后运行的结果的顺序按文件时间找到对应的hotspot_pid<PID>.log文件
//                .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:+TraceClassLoading", "-XX:+PrintAssembly")
                //  .addProfiler(CompilerProfiler.class)    // report JIT compiler profiling via standard MBeans
                //  .addProfiler(GCProfiler.class)    // report GC time
                // .addProfiler(StackProfiler.class) // report method stack execution profile
                // .addProfiler(PausesProfiler.class)
                /*
                WinPerfAsmProfiler
                You must install Windows Performance Toolkit. Once installed, locate directory with xperf.exe file
                and either add it to PATH environment variable, or set it to jmh.perfasm.xperf.dir system property.
                 */
                //.addProfiler(WinPerfAsmProfiler.class)
                //更多Profiler,请看JMH介绍
                .output("JMHTest.log")//输出信息到文件
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void test() {
        int sum = 0;
        for(int i = 0; i < 1000000; i++){
            sum += i;
        }
    }
}
