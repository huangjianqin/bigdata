package org.kin.bigdata.sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by huangjianqin on 2017/10/9.
  */
object SparkSQLInsert {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLInsert").setMaster("local[2]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext

    val originData = Seq(Person("AAAA", 12), Person("BBBB", 13))
    val data = sc.parallelize(originData)

    val originData1 = Seq(Person("CCCC", 14), Person("DDDD", 15))
    val data1 = sc.parallelize(originData1)

    import sparkSession.implicits._
    val dataDF = data.toDF()
    dataDF.createOrReplaceTempView("person")
    val dataDF1 = data1.toDF().union(sparkSession.sql("select * from person"))
    dataDF1.createOrReplaceTempView("person")

    sparkSession.sql("select * from person").show()

    sparkSession.stop()
    sc.stop()
  }
}

case class Person(name: String, age: Int){

}