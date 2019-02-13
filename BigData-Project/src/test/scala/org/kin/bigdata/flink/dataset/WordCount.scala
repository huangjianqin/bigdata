package org.kin.bigdata.flink.dataset

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by huangjianqin on 2019/1/8.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val params = ParameterTool.fromArgs(args)

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val text = env.readTextFile("data/words.txt")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()
    //有sink才需要
//    env.execute("WordCount")
  }
}
