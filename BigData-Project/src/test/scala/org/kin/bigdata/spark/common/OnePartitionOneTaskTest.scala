package org.kin.bigdata.spark.common

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.util.Random

/**
  * Created by huangjianqin on 2017/10/19.
  * 测试spark sortbykey 是每个分区有序还是全局有序
  */
object OnePartitionOneTaskTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("testSortByKey").setMaster("local[2]")
    val sc = new SparkContext(conf)

    def randomProduceTupleSeq(): immutable.IndexedSeq[(Int, Int)] ={
      return for(i <- Range(1, 100)) yield (Random.nextInt(i), Random.nextInt(i))
    }

    var rdd = sc.parallelize(randomProduceTupleSeq(), 2)
    rdd = rdd.sortByKey(numPartitions = 2)
    rdd.foreachPartition{iterator =>
      val sb = new StringBuilder()
      sb.append("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" + System.lineSeparator())
      for((t1, t2) <- iterator){
        sb.append("%s->%s".format(t1, t2) + System.lineSeparator())
      }
      sb.append(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + System.lineSeparator())
      println(sb.toString())
    }

    sc.stop()
  }
}
