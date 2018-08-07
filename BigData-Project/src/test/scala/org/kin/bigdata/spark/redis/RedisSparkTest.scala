package org.kin.bigdata.spark.redis

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by 健勤 on 2017/8/2.
 */
object RedisSparkTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Redis-Spark-Test")
      .setMaster("local[4]")
      .set("redis.host", "192.168.40.128")
      .set("redis.port", "6379")
//      .set("redis.auth", "")
    val sc = new SparkContext(conf)

    import com.redislabs.provider.redis._

    val rdd1 = sc.fromRedisKV("s1")
    val rdd2 = sc.fromRedisHash("hms")
    val rdd3 = sc.fromRedisList("l1")
    val rdd4 = sc.fromRedisSet("set")
    val rdd5 = sc.fromRedisZSet("zset")
    rdd1.foreach(println)
    rdd2.foreach(println)
    rdd3.foreach(println)
    rdd4.foreach(println)
    rdd5.foreach(println)

    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint(".checkpoint")
    val listItem = ssc.createRedisStream(Array("l1"), storageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
    listItem.print()
    ssc.awaitTermination()
  }
}
