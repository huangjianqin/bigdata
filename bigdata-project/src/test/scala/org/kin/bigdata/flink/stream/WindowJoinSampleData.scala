//package org.kin.bigdata.flink.stream
//
//import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
//import org.kin.bigdata.flink.stream.WindowJoin.{Grade, Salary}
//
//import scala.util.Random
//
///**
//  * Created by huangjianqin on 2019/1/7.
//  */
//object WindowJoinSampleData {
//  private val NAMES = Array("tom", "jerry", "alice", "bob", "john", "grace")
//  private val GRADE_COUNT = 5
//  private val SALARY_MAX = 10000
//
//  /**
//    * Continuously generates (name, grade).
//    */
//  def getGradeSource(env: StreamExecutionEnvironment): DataStream[Grade] = {
//    env.addSource(new ParallelSourceFunction[Grade] {
//      private var running = true
//      private[this] val rnd = new Random(hashCode())
//
//      override def cancel(): Unit = running = false
//
//      override def run(sourceContext: SourceFunction.SourceContext[Grade]): Unit = {
//        while(running && !Thread.currentThread().isInterrupted){
//          val grade = Grade(NAMES(rnd.nextInt(NAMES.length)), rnd.nextInt(GRADE_COUNT) + 1)
//          println(grade)
//          sourceContext.collect(grade)
//          Thread.sleep(1000L)
//        }
//      }
//    })
//  }
//
//  /**
//    * Continuously generates (name, salary).
//    */
//  def getSalarySource(env: StreamExecutionEnvironment): DataStream[Salary] = {
//    env.addSource(new ParallelSourceFunction[Salary] {
//      private var running = true
//      private[this] val rnd = new Random(hashCode())
//
//      override def cancel(): Unit = running = false
//
//      override def run(sourceContext: SourceFunction.SourceContext[Salary]): Unit = {
//        while(running && !Thread.currentThread().isInterrupted){
//          val salary = Salary(NAMES(rnd.nextInt(NAMES.length)), rnd.nextInt(SALARY_MAX) + 1)
//          println(salary)
//          sourceContext.collect(salary)
//          Thread.sleep(500L)
//        }
//      }
//    })
//  }
//}
