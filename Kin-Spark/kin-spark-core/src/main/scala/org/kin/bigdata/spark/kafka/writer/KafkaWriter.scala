package org.kin.bigdata.spark.kafka.writer

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}

import scala.reflect.ClassTag

/**
  * Created by huangjianqin on 2017/11/7.
  * 参考自https://github.com/BenFradet/spark-kafka-writer
  */
abstract class KafkaWriter[T: ClassTag] extends Serializable {
  def write2Kafka[K, V](transformFunc: T => ProducerRecord[K, V], callback: Option[Callback] = None)
                       (implicit producerConfig: Map[String, Object]): Unit
}
