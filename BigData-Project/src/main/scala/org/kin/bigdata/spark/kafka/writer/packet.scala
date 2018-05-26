package org.kin.bigdata.spark.kafka

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.streaming.dstream.DStream
//want to use specify implicit, must import this package or add compiler option language:implicitConversions
import scala.language.implicitConversions

import scala.reflect.ClassTag

/**
  * Created by huangjianqin on 2017/11/7.
  * 参考自https://github.com/BenFradet/spark-kafka-writer
  */
package object writer {
  implicit val producerConf = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer"
  )

  implicit def rdd2KafkaWriter[T: ClassTag](rdd: RDD[T]): KafkaWriter[T] = new RDDKafkaWriter[T](rdd)
  implicit def dstream2KafkaWriter[T: ClassTag](dstream: DStream[T]): KafkaWriter[T] = new DStreamKafkaWriter[T](dstream)
  implicit def dataset2KafkaWriter[T: ClassTag](dataset: Dataset[T]): KafkaWriter[T] = new DatasetKafkaWriter[T](dataset)
  implicit def dataframe2KafkaWriter(dataframe: DataFrame): KafkaWriter[Row] = new RDDKafkaWriter[Row](dataframe.rdd)
}
