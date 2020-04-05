//package org.kin.bigdata.flink.sql
//
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//import org.apache.flink.table.api.TableEnvironment
//import org.apache.flink.table.api.scala._
//
///**
//  * Created by huangjianqin on 2019/2/13.
//  */
//object StreamSQLExample {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val tEnv = TableEnvironment.getTableEnvironment(env)
//
//    val orderA: DataStream[Order] = env.fromCollection(Seq(
//      Order(1L, "beer", 3),
//      Order(1L, "diaper", 4),
//      Order(3L, "rubber", 2)))
//
//    val orderB: DataStream[Order] = env.fromCollection(Seq(
//      Order(2L, "pen", 3),
//      Order(2L, "rubber", 3),
//      Order(4L, "beer", 1)))
//
//    var tableA = tEnv.fromDataStream(orderA, 'user, 'product, 'amount)
//    tEnv.registerDataStream("OrderB", orderB, 'user, 'product, 'amount)
//
//    val result = tEnv.sqlQuery(
//      s"SELECT * FROM $tableA WHERE amount > 2 UNION ALL " +
//        "SELECT * FROM OrderB WHERE amount < 2")
//
//    result.toAppendStream[Order].print()
//
//    env.execute()
//  }
//
//  case class Order(user: Long, product: String, amount: Int)
//}
