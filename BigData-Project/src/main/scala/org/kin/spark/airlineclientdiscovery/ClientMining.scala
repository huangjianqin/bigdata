package org.kin.spark.airlineclientdiscovery

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by huangjianqin on 2017/8/27.
  */
object ClientMining {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ClientMining").setMaster("local[2]")
    val sc = new SparkContext(conf)

    try {
      val lrfmc = sc.textFile("data/filteredAirlineData")
      val lrfmcV = lrfmc.map { itemStr =>
        val lrfmcD = for (item <- itemStr.split(",")) yield item.toDouble
        Vectors.dense(lrfmcD)
      }
      //k-means||
      //random
      val model = KMeans.train(lrfmcV, 5, 50)
      println(model.computeCost(lrfmcV))
      val numInCluster = lrfmcV.map { v =>
        (model.predict(v), 1)
      }
        .reduceByKey(_ + _)
      val numInClusterArr = numInCluster.collect();
      println(numInClusterArr.mkString(","))
      println(model.clusterCenters.mkString(System.lineSeparator()))
    } finally {
      sc.stop()
    }
  }
}
