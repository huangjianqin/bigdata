package org.kin.bigdata.spark.kafka

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by huangjianqin on 2017/11/7.
  * 参考自https://github.com/BenFradet/spark-kafka-writer
  */
package object writer {
  implicit def rdd2KafkaWriter[T: ClassTag](rdd: RDD[T]): KafkaWriter[T] = new RDDKafkaWriter[T](rdd)
  implicit def dstream2KafkaWriter[T: ClassTag](dstream: DStream[T]): KafkaWriter[T] = new DStreamKafkaWriter[T](dstream)
  implicit def dataset2KafkaWriter[T: ClassTag](dataset: Dataset[T]): KafkaWriter[T] = new DatasetKafkaWriter[T](dataset)
  implicit def dataframe2KafkaWriter(dataframe: DataFrame): KafkaWriter[Row] = new RDDKafkaWriter[Row](dataframe.rdd)
}
