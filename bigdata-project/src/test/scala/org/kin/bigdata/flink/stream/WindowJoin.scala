//package org.kin.bigdata.flink.stream
//
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//
///**
//  * Created by huangjianqin on 2019/1/7.
//  */
//object WindowJoin {
//  case class Grade(name: String, grade: Int)
//
//  case class Salary(name: String, salary: Int)
//
//  case class Person(name: String, grade: Int, salary: Int)
//
//  def main(args: Array[String]): Unit = {
//    import org.apache.flink.api.scala._
//    val params = ParameterTool.fromArgs(args)
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
//
//    val grades = WindowJoinSampleData.getGradeSource(env)
//    val salaries = WindowJoinSampleData.getSalarySource(env)
//
//    val joined = grades.join(salaries)
//      .where(_.name)
//      .equalTo(_.name)
//      .window(TumblingEventTimeWindows.of(Time.milliseconds(2000L)))
//      .apply { (g, s) => Person(g.name, g.grade, s.salary) }
//
//    joined.print().setParallelism(1)
//
//    env.execute("WindowJoin")
//  }
//}
