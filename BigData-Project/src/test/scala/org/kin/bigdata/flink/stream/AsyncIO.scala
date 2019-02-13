package org.kin.bigdata.flink.stream

import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by huangjianqin on 2019/1/7.
  */
object AsyncIO {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val input = env.addSource(new SimpleSource())

    val asyncMapped = AsyncDataStream.orderedWait(input, 10000L, TimeUnit.MILLISECONDS, 10) {
      (input, collector: ResultFuture[Int]) =>
        Future {
          collector.complete(Seq(input))
        } (ExecutionContext.global)
    }

    asyncMapped.print()

    env.execute("AsyncIO")
  }

  class SimpleSource extends ParallelSourceFunction[Int] {
    var running = true
    var counter = 0

    override def run(ctx: SourceContext[Int]): Unit = {
      while (running) {
        ctx.getCheckpointLock.synchronized {
          ctx.collect(counter)
        }
        counter += 1

        Thread.sleep(1000L)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }
}
