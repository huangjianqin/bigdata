package org.kin.bigdata.flink.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Created by huangjianqin on 2019/1/3.
  */
object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val windowSize = params.getInt("window", 2)
    val slideSize = params.getInt("slide", 1)

    val text: DataStream[String] = env.socketTextStream("localhost", 55555)

    val counts: DataStream[(String, Int)] = text
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .countWindow(windowSize, slideSize)
      .sum(1)

    counts.print().setParallelism(1)

    env.execute("WindowWordCount")
  }
}
