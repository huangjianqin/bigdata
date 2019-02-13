package org.kin.bigdata.flink.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by huangjianqin on 2019/1/3.
  */
object SocketWordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    //nc -l 55555开启net服务
    val text: DataStream[String] = env.socketTextStream("localhost", 55555, '\n')

    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    windowCounts.print().setParallelism(1)

    env.execute("SocketWordCount")
  }

  case class WordWithCount(word: String, count: Long)
}
