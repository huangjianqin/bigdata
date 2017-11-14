package org.kin.bigdata.spark.kafka.writer

import java.beans.Transient

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

/**
  * Created by huangjianqin on 2017/11/7.
  * 参考自https://github.com/BenFradet/spark-kafka-writer
  */
class DatasetKafkaWriter[T: ClassTag](@Transient val dataset: Dataset[T]) extends KafkaWriter[T] with Serializable{
  override def write2Kafka[K, V](
                                  producerConfig: Map[String, Object],
                                  transformFunc: (T) => ProducerRecord[K, V],
                                  callback: Option[Callback]): Unit = {
    dataset.rdd.write2Kafka(producerConfig, transformFunc, callback)
  }
}
