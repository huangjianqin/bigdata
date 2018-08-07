package org.kin.bigdata.spark.kafka.writer

import org.apache.kafka.clients.producer._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by huangjianqin on 2018/1/1.
  */
object TestKafkaWriter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestKafkaWriter")
      val sc = new SparkContext(conf)

      class MyCallback extends Callback with Serializable{
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = println(metadata)
      }

      val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7))
      import org.kin.bigdata.spark.kafka.writer._
      rdd1.write2Kafka({data =>
        new ProducerRecord("test", null, data.toString)
    }, Option.apply(new MyCallback()))
  }
}
