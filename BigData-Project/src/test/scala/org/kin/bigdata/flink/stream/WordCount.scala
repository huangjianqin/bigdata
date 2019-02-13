package org.kin.bigdata.flink.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
  * Created by huangjianqin on 2018/12/9.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setGlobalJobParameters(params)

    val text = env.readTextFile("data/words.txt")
//    val text = env.fromCollection(List("java", "c", "java", "C"))

    val counts: DataStream[(String, Int)] = text
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    counts.print("WordCount")

//    counts.writeAsText("result/count")
//    val result = env.execute()
    val myOutput: Iterator[(String, Int)] = DataStreamUtils.collect(counts.javaStream).asScala
    println(myOutput.mkString(","))
  }
}
