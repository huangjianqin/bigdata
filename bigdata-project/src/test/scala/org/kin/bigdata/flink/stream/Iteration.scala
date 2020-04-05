//package org.kin.bigdata.flink.stream
//
//import java.util.Random
//
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.functions.source.SourceFunction
//import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
//import org.apache.flink.streaming.api.scala._
//
///**
//  * Created by huangjianqin on 2019/1/3.
//  */
//object Iteration {
//  private final val Bound = 100
//
//  def main(args: Array[String]): Unit = {
//    import org.apache.flink.api.scala._
//
//    val params = ParameterTool.fromArgs(args)
//
//    //设置网络buffer超时时间为1ms
//    val env = StreamExecutionEnvironment.getExecutionEnvironment.setBufferTimeout(1)
//    env.getConfig.setGlobalJobParameters(params)
//
//    val inputStream: DataStream[(Int, Int)] = env.addSource(new RandomFibonacciSource)
//
//    def withinBound(value: (Int, Int)) = value._1 < Bound && value._2 < Bound
//
//    val numbers: DataStream[((Int, Int), Int)] = inputStream
//      .map(value => (value._1, value._2, value._1, value._2, 0))
//      .iterate(
//        (iteration: DataStream[(Int, Int, Int, Int, Int)]) => {
//          //多次计算Fibonacci数字组合, 过滤掉两数字都在bound外的组合, 未被过滤的重新去计算下一Fibonacci数字组合
//          val step = iteration.map(value =>
//            (value._1, value._2, value._4, value._3 + value._4, value._5 + 1))
//          val feedback = step.filter(value => withinBound(value._3, value._4))
//          val output: DataStream[((Int, Int), Int)] = step
//            .filter(value => !withinBound(value._3, value._4))
//            .map(value => ((value._1, value._2), value._5))
//          (feedback, output)
//        }
//        //数据5s后失效
//        , 5000L
//      )
//
//    numbers.print()
//
//    env.execute("Iteration")
//  }
//
//  private class RandomFibonacciSource extends SourceFunction[(Int, Int)] {
//
//    val rnd = new Random()
//    var counter = 0
//    @volatile var isRunning = true
//
//    override def run(ctx: SourceContext[(Int, Int)]): Unit = {
//
//      while (isRunning && counter < Bound) {
//        val first = rnd.nextInt(Bound / 2 - 1) + 1
//        val second = rnd.nextInt(Bound / 2 - 1) + 1
//
//        ctx.collect((first, second))
//        counter += 1
//        Thread.sleep(50L)
//      }
//    }
//
//    override def cancel(): Unit = isRunning = false
//  }
//}
