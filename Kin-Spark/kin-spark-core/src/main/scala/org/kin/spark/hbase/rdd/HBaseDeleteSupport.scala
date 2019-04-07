package org.kin.spark.hbase.rdd

import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.kin.hbase.core.entity.HBaseEntity
import org.kin.hbase.core.utils.HBaseUtils
import org.kin.spark.hbase._
import org.kin.spark.hbase.rdd.HBaseDeleteMethods._
//want to use specify implicit, must import this package or add compiler option language:implicitConversions
import scala.language.implicitConversions

/**
  * Created by huangjianqin on 2019/3/31.
  */
/**
  * Adds implicit methods to
  * `RDD[K]`,
  * `RDD[(K, Set[Q])]`,
  * `RDD[(K, Set[(Q, Long)])]`,
  * `RDD[(K, Map[String, Set[Q]])]`,
  * `RDD[(K, Map[String, Set[(Q, Long)]])]`,
  * `RDD[E <: HBaseEntity]`,
  * to delete HBase tables.
  */
trait HBaseDeleteSupport {
  implicit def toHBaseDeleteKeyRDD[K: Writer](rdd: RDD[K]): HBaseDeleteKeyRDD[K] =
    new HBaseDeleteKeyRDD(rdd)

  implicit def toHBaseDeleteSimpleRDD[K: Writer, Q: Writer](rdd: RDD[(K, Set[Q])]): HBaseDeleteSimpleRDD[K, Q] =
    new HBaseDeleteSimpleRDD(rdd, del[Q])

  implicit def toHBaseDeleteSimpleRDDT[K: Writer, Q: Writer](rdd: RDD[(K, Set[(Q, Long)])]): HBaseDeleteSimpleRDD[K, (Q, Long)] =
    new HBaseDeleteSimpleRDD(rdd, delT[Q])

  implicit def toHBaseDeleteRDD[K: Writer, Q: Writer](rdd: RDD[(K, Map[String, Set[Q]])]): HBaseDeleteRDD[K, Q] =
    new HBaseDeleteRDD(rdd, del[Q])

  implicit def toHBaseDeleteRDDT[K: Writer, Q: Writer](rdd: RDD[(K, Map[String, Set[(Q, Long)]])]): HBaseDeleteRDD[K, (Q, Long)] =
    new HBaseDeleteRDD(rdd, delT[Q])
  implicit def toHBaseDeleteHBaseEntityRDD[E <: HBaseEntity](rdd: RDD[E]): HBaseDeleteHBaseEntityRDD[E] = new HBaseDeleteHBaseEntityRDD[E](rdd)
}

private[hbase] object HBaseDeleteMethods {
  type Deleter[Q] = (Delete, Array[Byte], Q) => Delete

  // Delete
  def del[Q](delete: Delete, cf: Array[Byte], q: Q)(implicit wq: Writer[Q]): Delete = delete.addColumns(cf, wq.write(q))

  def delT[Q](delete: Delete, cf: Array[Byte], qt: (Q, Long))(implicit wq: Writer[Q]): Delete = delete.addColumn(cf, wq.write(qt._1), qt._2)
}

sealed abstract class HBaseDeleteHelper {
  protected def convert[K, Q](id: K, values: Map[String, Set[Q]], del: Deleter[Q])
                             (implicit wk: Writer[K], ws: Writer[String]): Option[(ImmutableBytesWritable, Delete)] = {
    val d = new Delete(wk.write(id))
    var empty = true
    for {
      (family, contents) <- values
      fb = ws.write(family)
      content <- contents
    } {
      empty = false
      del(d, fb, content)
    }

    if (empty) None else Some(new ImmutableBytesWritable, d)
  }

  protected def convert[K](id: K, families: Set[String])
                          (implicit wk: Writer[K], ws: Writer[String]): Option[(ImmutableBytesWritable, Delete)] = {
    val d = new Delete(wk.write(id))
    for (family <- families) d.addFamily(ws.write(family))
    Some(new ImmutableBytesWritable, d)
  }

  protected def convert[K](id: K)(implicit wk: Writer[K]): Option[(ImmutableBytesWritable, Delete)] = {
    val d = new Delete(wk.write(id))
    Some(new ImmutableBytesWritable, d)
  }
}

/**
  * @tparam K rowkey
  */
final class HBaseDeleteKeyRDD[K: Writer](val rdd: RDD[K]) extends HBaseDeleteHelper with Serializable {

  /**
    * 删除某rowkey
    */
  def deleteHBase(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
    * 删除某rowkey下某些column families的数据
    */
  def deleteHBase(table: String, families: Set[String])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, families) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
    * 删除某rowkey下某column family下某些columns的数据
    */
  def deleteHBase[Q: Writer](table: String, family: String, columns: Set[Q])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, Map(family -> columns), del[Q]) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  /**
    * 删除某rowkey下某些column families下某些columns的数据
    */
  def deleteHBase[Q: Writer](table: String, qualifiers: Map[String, Set[Q]])(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ k => convert(k, qualifiers, del[Q]) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

/**
  * @tparam K rowkey
  * @tparam Q columns
  */
final class HBaseDeleteSimpleRDD[K: Writer, Q](val rdd: RDD[(K, Set[Q])], val del: Deleter[Q]) extends HBaseDeleteHelper with Serializable {
  /**
    * 删除某column family下某些columns的数据
    */
  def deleteHBase(table: String, family: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, Map(family -> v), del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

/**
  * @tparam K rowkey
  * String  -> column family
  * @tparam Q columns
  */
final class HBaseDeleteRDD[K: Writer, Q](val rdd: RDD[(K, Map[String, Set[Q]])], val del: Deleter[Q]) extends HBaseDeleteHelper with Serializable {
  /**
    * 删除某些column families下某些columns的数据
    */
  def deleteHBase(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.flatMap({ case (k, v) => convert(k, v, del) }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}

final class HBaseDeleteHBaseEntityRDD[E <: HBaseEntity](val rdd: RDD[E]) extends Serializable{
  /**
    * 删除HBaseEntity
    */
  def deleteHBaseEntity(table: String)(implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = createJob(table, conf)

    rdd.map(e => {
      val d = new Delete(HBaseUtils.getRowKeyBytes(e))
      (new ImmutableBytesWritable, d)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}