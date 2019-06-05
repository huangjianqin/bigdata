package org.kin.spark.kafka.writer

import java.util.concurrent.{Callable, ExecutionException, TimeUnit}

import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Created by huangjianqin on 2017/11/7.
  * 参考自https://github.com/BenFradet/spark-kafka-writer
  */
object KafkaProducerCache {
  private type ProducerConfig = Map[String, Object]
  private type ExProucer = KafkaProducer[_, _]

  private val removalListener = new RemovalListener[ProducerConfig, ExProucer] {
    override def onRemoval(removalNotification: RemovalNotification[ProducerConfig, ExProucer]) = {
      removalNotification.getValue.close()
    }
  }

  private val cacheExpireTimeout = 5.minutes.toMillis
  private val cache = CacheBuilder.newBuilder()
    .expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
    .removalListener(removalListener)
    .build[ProducerConfig, ExProucer]()


  def getProducer[K, V](producerConfig: Map[String, Object]): KafkaProducer[K, V] = {
    try {
      cache.get(producerConfig, new Callable[KafkaProducer[K, V]] {
        override def call(): KafkaProducer[K, V] = new KafkaProducer[K, V](producerConfig.asJava)
      }).asInstanceOf[KafkaProducer[K, V]]
    } catch {
      case e@(_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null => throw e.getCause
    }
  }

  def close(producerConfig: Map[String, Object]) = cache.invalidate(producerConfig)

}
