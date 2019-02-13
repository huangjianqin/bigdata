package org.kin.bigdata.flink.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by huangjianqin on 2019/1/4.
  */
object SessionWindow {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val param = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(param)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val input = List(
      ("a", 1L, 1),
      ("b", 1L, 1),
      ("b", 3L, 1),
      ("b", 5L, 1),
      ("c", 6L, 1),
      ("a", 10L, 1),
      ("c", 11L, 1)
    )

    val source: DataStream[(String, Long, Int)] = env.addSource(
      new SourceFunction[(String, Long, Int)]() {

        override def run(ctx: SourceContext[(String, Long, Int)]): Unit = {
          input.foreach(value => {
            ctx.collectWithTimestamp(value, value._2)
            ctx.emitWatermark(new Watermark(value._2 - 1))
          })
          ctx.emitWatermark(new Watermark(Long.MaxValue))
        }

        override def cancel(): Unit = {}

      })

    val aggregated: DataStream[(String, Long, Int)] = source
      .keyBy(0)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
      .sum(2)

    aggregated.print()

    env.execute("SessionWindow")
  }
}
