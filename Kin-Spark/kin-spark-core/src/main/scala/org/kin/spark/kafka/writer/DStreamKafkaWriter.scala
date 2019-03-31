package org.kin.spark.kafka.writer

import java.beans.Transient

import org.apache.kafka.clients.producer.{Callback, ProducerRecord}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by huangjianqin on 2017/11/7.
  * 参考自https://github.com/BenFradet/spark-kafka-writer
  */
class DStreamKafkaWriter[T: ClassTag](@Transient val dstream: DStream[T]) extends KafkaWriter[T] with Serializable {
  override def write2Kafka[K, V](transformFunc: (T) => ProducerRecord[K, V], callback: Option[Callback])
                                (implicit producerConfig: Map[String, Object]): Unit = {
    dstream.foreachRDD { rdd =>
      rdd.write2Kafka(transformFunc, callback)(producerConfig)
    }
  }
}
