package org.kin.bigdata.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by huangjianqin on 2017/10/9.
  */
object SparkSQLInsert {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLInsert").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)

    val originData = Seq(Person("AAAA", 12), Person("BBBB", 13))
    val data = sc.parallelize(originData)

    val originData1 = Seq(Person("CCCC", 14), Person("DDDD", 15))
    val data1 = sc.parallelize(originData1)

    import ssc.implicits._
    val dataDF = data.toDF()
    dataDF.registerTempTable("person")
    val dataDF1 = data1.toDF().unionAll(ssc.sql("select * from person"))
    dataDF1.registerTempTable("person")

    ssc.sql("select * from person").show()

    sc.stop()
  }
}

case class Person(name: String, age: Int){

}