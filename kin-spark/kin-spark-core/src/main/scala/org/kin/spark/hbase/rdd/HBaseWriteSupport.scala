package org.kin.spark.hbase.rdd

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.kin.hbase.core.entity.HBaseEntity
import org.kin.hbase.core.utils.HBaseUtils
import org.kin.spark.hbase._
import org.kin.spark.hbase.rdd.HBaseWriteMethods._
//want to use specify implicit, must import this package or add compiler option language:implicitConversions
import scala.language.implicitConversions

/**
  * Created by huangjianqin on 2019/3/31.
  */
/**
  * Adds implicit methods to
  * `RDD[(K, Map[Q, V])]`,
  * `RDD[(K, Seq[V])]`,
  * `RDD[(K, Map[String, Map[Q, V]])]`
  * `RDD[(K, Map[Q, (V, Long)])]`,
  * `RDD[(K, Seq[(V, Long)])]`,
  * `RDD[(K, Map[String, Map[Q, (V, Long)]])]`,
  * `RDD[E <: HBaseEntity]`,
  * to write to HBase tables.
  */
trait HBaseWriteSupport {
  implicit def toRDD2HBaseSimple[K: Writer, Q: Writer, V: Writer](rdd: RDD[(K, Map[Q, V])]): RDD2HBaseSimple[K, Q, V] =
    new RDD2HBaseSimple(rdd, pa[V])

  implicit def toRDD2HBaseSimpleTS[K: Writer, Q: Writer, V: Writer](rdd: RDD[(K, Map[Q, (V, Long)])]): RDD2HBaseSimple[K, Q, (V, Long)] =
    new RDD2HBaseSimple(rdd, pa[V])

  implicit def toRDD2HBaseFixed[K: Writer, V: Writer](rdd: RDD[(K, Seq[V])]): RDD2HBaseFixed[K, V] =
    new RDD2HBaseFixed(rdd, pa[V])

  implicit def toRDD2HBaseFixedTS[K: Writer, V: Writer](rdd: RDD[(K, Seq[(V, Long)])]): RDD2HBaseFixed[K, (V, Long)] =
    new RDD2HBaseFixed(rdd, pa[V])

  implicit def toRDD2HBase[K: Writer, Q: Writer, V: Writer](rdd: RDD[(K, Map[String, Map[Q, V]])]): RDD2HBase[K, Q, V] =
    new RDD2HBase(rdd, pa[V])

  implicit def toRDD2HBaseT[K: Writer, Q: Writer, V: Writer](rdd: RDD[(K, Map[String, Map[Q, (V, Long)]])]): RDD2HBase[K, Q, (V, Long)] =
    new RDD2HBase(rdd, pa[V])

  implicit def toHBaseEntity2HBase[E <: HBaseEntity](rdd: RDD[E]): HBaseEntity2HBase[E] = new HBaseEntity2HBase[E](rdd)
}

private[rdd] object HBaseWriteMethods {
  type PutAdder[V] = (Put, Array[Byte], Array[Byte], V) => Put

  // PutAdder
  def pa[V](put: Put, cf: Array[Byte], q: Array[Byte], v: V)(implicit wv: Writer[V]): Put = put.addColumn(cf, q, wv.write(v))

  def pa[V](put: Put, cf: Array[Byte], q: Array[Byte], v: (V, Long))(implicit wv: Writer[V]): Put = put.addColumn(cf, q, v._2, wv.write(v._1))
}

sealed abstract class HBaseWriteHelper {
  protected def convert[K, Q, V](id: K, values: Map[String, Map[Q, V]], put: PutAdder[V])
                                (implicit wk: Writer[K], wq: Writer[Q], ws: Writer[String]): Option[(ImmutableBytesWritable, Put)] = {
    val p = new Put(wk.write(id))
    var empty = true
    for {
      (family, content) <- values
      fb = ws.write(family)
      (qualifier, value) <- content
    } {
      empty = false
      put(p, fb, wq.write(qualifier), value)
    }

    if (empty) None else Some(new ImmutableBytesWritable, p)
  }
}

final class RDD2HBaseSimple[K: Writer, Q: Writer, V](val rdd: RDD[(K, Map[Q, V])], val put: PutAdder[V]) extends HBaseWriteHelper with Serializable {
  /**
    * 简单版本, 所有数据都来自于同一column family
    */
  def toHBase(table: String, family: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class RDD2HBaseFixed[K: Writer, V](val rdd: RDD[(K, Seq[V])], val put: PutAdder[V]) extends HBaseWriteHelper with Serializable {
  /**
    * 简单版本, 所有数据都来自于同一column family, 并且column是固定的
    */
  def toHBase[Q: Writer](table: String, family: String, headers: Seq[Q])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    val sc = rdd.context
    val bheaders = sc.broadcast(headers)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> Map(bheaders.value zip v: _*)), put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class RDD2HBase[K: Writer, Q: Writer, V](val rdd: RDD[(K, Map[String, Map[Q, V]])], val put: PutAdder[V]) extends HBaseWriteHelper with Serializable {
  /**
    * 通用版本
    */
  def toHBase(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, put) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseEntity2HBase[E <: HBaseEntity](val rdd: RDD[E]) extends Serializable {
  /**
    * 写HBaseEntity到HBase
    */
  def toHBase(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.map({e => (new ImmutableBytesWritable, HBaseUtils.convert2Puts(e).get(0))}).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}