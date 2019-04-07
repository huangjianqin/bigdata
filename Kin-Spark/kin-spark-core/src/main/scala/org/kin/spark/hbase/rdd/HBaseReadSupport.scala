package org.kin.spark.hbase.rdd

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{IdentityTableMapper, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.kin.hbase.core.entity.HBaseEntity
import org.kin.hbase.core.utils.HBaseUtils
//want to use specify implicit, must import this package or add compiler option language:implicitConversions
import scala.language.implicitConversions

/**
  * Created by huangjianqin on 2019/3/31.
  */
trait HBaseReadSupport {
  implicit def toHBaseReader(sc: SparkContext): HBaseReader = new HBaseReader(sc)
}

final class HBaseReader(@transient sc: SparkContext) extends Serializable {
  private def extract[Q, V](data: Map[String, Set[Q]], result: Result, read: Cell => V)(implicit wq: Writer[Q], ws: Writer[String]) = {
    data map {
      case (cf, columns) =>
        val content = columns flatMap { column =>
          Option {
            result.getColumnLatestCell(ws.write(cf), wq.write(column))
          } map { cell =>
            column -> read(cell)
          }
        } toMap

        cf -> content
    }
  }

  private def extractRow[Q, V](data: Set[String], result: Result, read: Cell => V)(implicit rq: Reader[Q], rs: Reader[String]) = {
    result.listCells groupBy { cell =>
      rs.read(CellUtil.cloneFamily(cell))
    } filterKeys data.contains map {
      // We cannot use mapValues here, because it returns a MapLike, which is not serializable,
      // instead we need a (serializable) Map (see https://issues.scala-lang.org/browse/SI-7005)
      case (k, cells) =>
        (k, cells map { cell =>
          val column = rq.read(CellUtil.cloneQualifier(cell))
          column -> read(cell)
        } toMap)
    }
  }

  private def read[V](cell: Cell)(implicit rv: Reader[V]) = {
    val value = CellUtil.cloneValue(cell)
    rv.read(value)
  }

  private def readTS[V](cell: Cell)(implicit rv: Reader[V]) = {
    val value = CellUtil.cloneValue(cell)
    val timestamp = cell.getTimestamp
    (rv.read(value), timestamp)
  }

  private def makeConf(config: HBaseConfig, table: String, columns: Option[String] = None, scan: Scan = new Scan) = {
    val conf = config.get

    if (columns.isDefined)
      conf.set(TableInputFormat.SCAN_COLUMNS, columns.get)

    val job = Job.getInstance(conf)
    TableMapReduceUtil.initTableMapperJob(table, scan, classOf[IdentityTableMapper], null, null, job)

    job.getConfiguration
  }

  private def prepareScan(filter: Filter) = new Scan().setFilter(filter)

  /**
    * @param data 代表着需要读取的column families及其column qualifier
    * @return `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]` or `RDD[(String, Map[String, Map[String, V]])]`
    */
  def hbase[K: Reader, Q: Writer, V: Reader](table: String, data: Map[String, Set[Q]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, new Scan)

  def hbase[K: Reader, V: Reader](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, new Scan)

  def hbase[V: Reader](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, new Scan)

  /**
    * @param data 代表着需要读取的column families及其column qualifier
    * @return `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]` or `RDD[(String, Map[String, Map[String, V]])]`
    * 支持filter
    */
  def hbase[K: Reader, Q: Writer, V: Reader](table: String, data: Map[String, Set[Q]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, prepareScan(filter))

  def hbase[K: Reader, V: Reader](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, prepareScan(filter))

  def hbase[V: Reader](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, prepareScan(filter))

  /**
    * @param data 代表着需要读取的column families及其column qualifier
    * @return `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]` or `RDD[(String, Map[String, Map[String, V]])]`
    * 支持自定义Scan
    */
  def hbase[K: Reader, Q: Writer, V: Reader](table: String, data: Map[String, Set[Q]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] = {
    val rk = implicitly[Reader[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extract(data, row, read[V])
    }
  }

  def hbase[K: Reader, V: Reader](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, scan)

  def hbase[V: Reader](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, scan)

  /**
    * @param data 代表着需要读取的column families及其column qualifier
    * @return `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]` or `RDD[(String, Map[String, Map[String, (V, Long)]])]`
    */
  def hbaseTS[K: Reader, Q: Writer, V: Reader](table: String, data: Map[String, Set[Q]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, new Scan)

  def hbaseTS[K: Reader, V: Reader](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, new Scan)

  def hbaseTS[V: Reader](table: String, data: Map[String, Set[String]])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, new Scan)

  /**
    * @param data 代表着需要读取的column families及其column qualifier
    * @return `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]` or `RDD[(String, Map[String, Map[String, (V, Long)]])]`
    * 支持filter
    */
  def hbaseTS[K: Reader, Q: Writer, V: Reader](table: String, data: Map[String, Set[Q]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, prepareScan(filter))

  def hbaseTS[K: Reader, V: Reader](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, prepareScan(filter))

  def hbaseTS[V: Reader](table: String, data: Map[String, Set[String]], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, prepareScan(filter))

  /**
    * @param data 代表着需要读取的column families及其column qualifier
    * @return `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]` or `RDD[(String, Map[String, Map[String, (V, Long)]])]`
    * 支持自定义Scan
    */
  def hbaseTS[K: Reader, Q: Writer, V: Reader](table: String, data: Map[String, Set[Q]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] = {
    val rk = implicitly[Reader[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extract(data, row, readTS[V])
    }
  }

  def hbaseTS[K: Reader, V: Reader](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, scan)

  def hbaseTS[V: Reader](table: String, data: Map[String, Set[String]], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, scan)

  protected def hbaseRaw[Q: Writer](table: String, data: Map[String, Set[Q]], scan: Scan)(implicit config: HBaseConfig): RDD[(ImmutableBytesWritable, Result)] = {
    val columns = (for {
      (cf, cols) <- data
      col <- cols
    } yield s"$cf:$col") mkString " "

    sc.newAPIHadoopRDD(makeConf(config, table, Some(columns), scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
  }

  /**
    * @param data 需要读取的column families
    * @return `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]` or `RDD[(String, Map[String, Map[String, V]])]`
    */
  def hbase[K: Reader, Q: Reader, V: Reader](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, new Scan)

  def hbase[K: Reader, V: Reader](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, new Scan)

  def hbase[V: Reader](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, new Scan)

  /**
    * @param data 需要读取的column families
    * @return `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]` or `RDD[(String, Map[String, Map[String, V]])]`
    * 支持filter
    */
  def hbase[K: Reader, Q: Reader, V: Reader](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] =
    hbase(table, data, prepareScan(filter))

  def hbase[K: Reader, V: Reader](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, prepareScan(filter))

  def hbase[V: Reader](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, prepareScan(filter))

  /**
    * @param data 需要读取的column families
    * @return `RDD[(K, Map[String, Map[Q, V]])]` or `RDD[(K, Map[String, Map[String, V]])]` or `RDD[(String, Map[String, Map[String, V]])]`
    * 支持自定义Scan
    */
  def hbase[K: Reader, Q: Reader, V: Reader](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, V]])] = {
    val rk = implicitly[Reader[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extractRow(data, row, read[V])
    }
  }

  def hbase[K: Reader, V: Reader](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, V]])] =
    hbase[K, String, V](table, data, scan)

  def hbase[V: Reader](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, V]])] =
    hbase[String, String, V](table, data, scan)

  /**
    * @param data 需要读取的column families
    * @return `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]` or `RDD[(String, Map[String, Map[String, (V, Long)]])]`
    */
  def hbaseTS[K: Reader, Q: Reader, V: Reader](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, new Scan)

  def hbaseTS[K: Reader, V: Reader](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, new Scan)

  def hbaseTS[V: Reader](table: String, data: Set[String])(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, new Scan)

  /**
    * @param data 需要读取的column families
    * @return `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]` or `RDD[(String, Map[String, Map[String, (V, Long)]])]`
    * 支持filter
    */
  def hbaseTS[K: Reader, Q: Reader, V: Reader](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] =
    hbaseTS(table, data, prepareScan(filter))

  def hbaseTS[K: Reader, V: Reader](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, prepareScan(filter))

  def hbaseTS[V: Reader](table: String, data: Set[String], filter: Filter)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, prepareScan(filter))

  /**
    * @param data 需要读取的column families
    * @return `RDD[(K, Map[String, Map[Q, (V, Long)]])]` or `RDD[(K, Map[String, Map[String, (V, Long)]])]` or `RDD[(String, Map[String, Map[String, (V, Long)]])]`
    * 支持自定义Scan
    */
  def hbaseTS[K: Reader, Q: Reader, V: Reader](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[Q, (V, Long)]])] = {
    val rk = implicitly[Reader[K]]
    hbaseRaw(table, data, scan) map {
      case (key, row) =>
        rk.read(key.get) -> extractRow(data, row, readTS[V])
    }
  }

  def hbaseTS[K: Reader, V: Reader](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(K, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[K, String, V](table, data, scan)

  def hbaseTS[V: Reader](table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(String, Map[String, Map[String, (V, Long)]])] =
    hbaseTS[String, String, V](table, data, scan)

  protected def hbaseRaw(table: String, data: Set[String], scan: Scan)(implicit config: HBaseConfig): RDD[(ImmutableBytesWritable, Result)] = {
    val families = data mkString " "

    sc.newAPIHadoopRDD(makeConf(config, table, Some(families), scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
  }

  /**
    * 读取HBase数据, 不解析其返回的Result
    * @return `RDD[(K, Result)]`
    */
  def hbase[K: Reader](table: String, scan: Scan = new Scan)(implicit config: HBaseConfig): RDD[(K, Result)] = {
    val rk = implicitly[Reader[K]]
    sc.newAPIHadoopRDD(makeConf(config, table, scan = scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
      case (key, row) =>
        rk.read(key.get) -> row
    }
  }

  /**
    * 读取HBase数据, 不解析其返回的Result
    * @return `RDD[(K, Result)]`
    * 支持filter
    */
  def hbase[K: Reader](table: String, filter: Filter)(implicit config: HBaseConfig): RDD[(K, Result)] = hbase(table, prepareScan(filter))

  /**
    * 转换成HBaseEntity
    */
  def hbaseEntity[K: Reader, E <: HBaseEntity](table: String, claxx: Class[E], scan: Scan = new Scan)(implicit config: HBaseConfig): RDD[(K, E)] = {
    val rk = implicitly[Reader[K]]
    sc.newAPIHadoopRDD(makeConf(config, table, scan = scan), classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result]) map {
      case (key, row) =>
        rk.read(key.get) -> HBaseUtils.convert2HBaseEntity(claxx, row)
    }
  }

  /**
    * 转换成HBaseEntity
    * 支持filter
    */
  def hbaseEntity[K: Reader, E <: HBaseEntity](table: String, claxx: Class[E], filter: Filter)(implicit config: HBaseConfig): RDD[(K, E)] = hbaseEntity(table, claxx, prepareScan(filter))
}