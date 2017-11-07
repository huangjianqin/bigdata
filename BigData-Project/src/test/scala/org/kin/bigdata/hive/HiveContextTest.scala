package org.kin.bigdata.hive

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by 健勤 on 2017/8/6.
 */
object HiveContextTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("HiveContextTest").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val movies = hiveContext.sql("select * from movie_info")
    movies.foreach(println(_))

    sc.stop()
  }
}
