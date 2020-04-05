//package org.kin.bigdata.flink.sql
//
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.api.scala._
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.table.api.scala._
//
///**
//  * Created by huangjianqin on 2019/1/9.
//  */
//object WordCountSQL {
//  def main(args: Array[String]): Unit = {
//    val params = ParameterTool.fromArgs(args)
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setGlobalJobParameters(params)
//
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//
//    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
//
//    //-----------------SQL-------------------------
//    tEnv.registerDataSet("WordCount", input, 'word, 'frequency)
//    val table = tEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")
//    table.toDataSet[WC].print()
//
//    //-----------------Table-------------------------
////    val expr = input.toTable(tEnv)
////    val result = expr
////      .groupBy('word)
////      .select('word, 'frequency.sum as 'frequency)
////      .filter('frequency === 2)
////      .toDataSet[WC]
////
////    result.print()
//  }
//
//  case class WC(word: String, frequency: Long)
//}
