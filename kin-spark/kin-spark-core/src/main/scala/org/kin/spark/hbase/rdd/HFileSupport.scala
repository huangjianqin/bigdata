package org.kin.spark.hbase.rdd

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.kin.hbase.core.entity.HBaseEntity
import org.kin.hbase.core.utils.HBaseUtils
import org.kin.spark.hbase.rdd.HFileMethods._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
//want to use specify implicit, must import this package or add compiler option language:implicitConversions
import scala.language.implicitConversions

/**
  * Created by huangjianqin on 2019/3/31.
  *
  * 导入大量数据, 直接写HFILE比写大量PUT要高效
  *
  * 使用MapReduce作业以HBase内部数据格式输出表格数据，然后直接将生成的StoreFiles加载到正在运行的集群中
  * 利用HBase数据按照HFile格式存储在HDFS的原理，使用Mapreduce直接生成HFile格式文件后，RegionServers再将HFile文件移动到相应的Region目录下
  * 优点:
  * 1.导入过程不占用Region资源
  * 2.快速导入海量的数据
  * 3.减少CPU网络内存资源消耗
  *
  * !!!!!在这种情况下,我们不用自己写reduce过程,但是会使用Hbase给我们提供的reduce,也就是说,无论你怎么设置reduce数量,都是无效的
  * !!!!!在建表的时候进行合理的预分区!!!预分区的数目会决定你的reduce过程的数目!简单来说,在一定的范围内,进行合适预分区的话,reduce的数量增加多少,效率就提高多少倍!!! ===> SPLITS
  * !!!!!如果hdfs地址没有写端口，会认为是两个不同文件系统，所以要copy，而如果当是同一文件系统时，则是move
  * !!!!!由于BulkLoad是绕过了Write to WAL，Write to MemStore及Flush to disk的过程，所以并不能通过WAL来进行一些复制数据的操作
  *
  */
trait HFileSupport {
  //隐式引用, 用于Spark分区内排序
  implicit lazy val cellKeyOrdering = new CellKeyOrdering
  implicit lazy val cellKeyTSOrdering = new CellKeyTSOrdering

  implicit def toHFileRDDSimple[K: Writer, Q: Writer, A: ClassTag](rdd: RDD[(K, Map[Q, A])])(implicit writer: Writer[A]): HFileRDDSimple[K, Q, CellKey, A, A] =
    new HFileRDDSimple[K, Q, CellKey, A, A](rdd, gc[A], kvf[A])

  implicit def toHFileRDDSimpleTS[K: Writer, Q: Writer, A: ClassTag](rdd: RDD[(K, Map[Q, (A, Long)])])(implicit writer: Writer[A]): HFileRDDSimple[K, Q, CellKeyTS, (A, Long), A] =
    new HFileRDDSimple[K, Q, CellKeyTS, (A, Long), A](rdd, gc[A], kvft[A])

  implicit def toHFileRDDFixed[K: Writer, A: ClassTag](rdd: RDD[(K, Seq[A])])(implicit writer: Writer[A]): HFileRDDFixed[K, CellKey, A, A] =
    new HFileRDDFixed[K, CellKey, A, A](rdd, gc[A], kvf[A])

  implicit def toHFileRDDFixedTS[K: Writer, A: ClassTag](rdd: RDD[(K, Seq[(A, Long)])])(implicit writer: Writer[A]): HFileRDDFixed[K, CellKeyTS, (A, Long), A] =
    new HFileRDDFixed[K, CellKeyTS, (A, Long), A](rdd, gc[A], kvft[A])

  implicit def toHFileRDD[K: Writer, Q: Writer, A: ClassTag](rdd: RDD[(K, Map[String, Map[Q, A]])])(implicit writer: Writer[A]): HFileRDD[K, Q, CellKey, A, A] =
    new HFileRDD[K, Q, CellKey, A, A](rdd, gc[A], kvf[A])

  implicit def toHFileRDDTS[K: Writer, Q: Writer, A: ClassTag](rdd: RDD[(K, Map[String, Map[Q, (A, Long)]])])(implicit writer: Writer[A]): HFileRDD[K, Q, CellKeyTS, (A, Long), A] =
    new HFileRDD[K, Q, CellKeyTS, (A, Long), A](rdd, gc[A], kvft[A])

  implicit def toHEntityHFileRDD[E <: HBaseEntity](entityRdd: RDD[E]): HBaseEntityHFileRDD[E, CellKey] =
    new HBaseEntityHFileRDD[E, CellKey](entityRdd, rowGC, rowKVF)
}

private[hbase] object HFileMethods {

  type CellKey = (Array[Byte], Array[Byte]) // (key, qualifier)
  type CellKeyTS = (CellKey, Long) // (CellKey, timestamp)

  type GetCellKey[C, A, V] = (CellKey, A) => (C, V)
  type KeyValueWrapper[C, V] = (C, V) => (ImmutableBytesWritable, KeyValue)
  //参数是列族
  type KeyValueWrapperF[C, V] = (Array[Byte]) => KeyValueWrapper[C, V]

  // GetCellKey
  def gc[A](c: CellKey, v: A): (CellKey, A) = (c, v)

  def rowGC(c: CellKey, vb: Array[Byte]): (CellKey, Array[Byte]) = (c, vb)

  def gc[A](c: CellKey, v: (A, Long)): (CellKeyTS, A) = ((c, v._2), v._1)

  //获取(ImmutableBytesWritable, KeyValue)
  // KeyValueWrapperF
  def kvf[A](f: Array[Byte])(c: CellKey, v: A)(implicit writer: Writer[A]): (ImmutableBytesWritable, KeyValue) =
    (new ImmutableBytesWritable(c._1), new KeyValue(c._1, f, c._2, writer.write(v)))

  def rowKVF(f: Array[Byte])(c: CellKey, vb: Array[Byte]): (ImmutableBytesWritable, KeyValue) =
    (new ImmutableBytesWritable(c._1), new KeyValue(c._1, f, c._2, vb))

  def kvft[A](f: Array[Byte])(c: CellKeyTS, v: A)(implicit writer: Writer[A]): (ImmutableBytesWritable, KeyValue) =
    (new ImmutableBytesWritable(c._1._1), new KeyValue(c._1._1, f, c._1._2, c._2, writer.write(v)))

  //用于Spark分区内排序
  class CellKeyOrdering extends Ordering[CellKey] {
    override def compare(a: CellKey, b: CellKey): Int = {
      val (ak, aq) = a
      val (bk, bq) = b
      // compare keys
      val ord = Bytes.compareTo(ak, bk)
      if (ord != 0) ord
      else Bytes.compareTo(aq, bq) // compare qualifiers
    }
  }

  //用于Spark分区内排序
  class CellKeyTSOrdering extends Ordering[CellKeyTS] {
    val cellKeyOrdering = new CellKeyOrdering

    override def compare(a: CellKeyTS, b: CellKeyTS): Int = {
      val (ac, at) = a
      val (bc, bt) = b
      val ord = cellKeyOrdering.compare(ac, bc)
      if (ord != 0) ord
      else {
        // see org.apache.hadoop.hbase.KeyValue.KVComparator.compareTimestamps(long, long)
        if (at < bt) 1
        else if (at > bt) -1
        else 0
      }
    }
  }

}

sealed abstract class HFileRDDHelper extends Serializable {

  private object HFilePartitioner {
    def apply(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegionPerFamily: Int): HFilePartitioner = {
      //每个region的HFile不允许超过BulkLoadHFiles.MAX_FILES_PER_REGION_PER_FAMILY(默认32), 否则会写入失败
      val fraction = 1 max numFilesPerRegionPerFamily min conf.getInt(BulkLoadHFiles.MAX_FILES_PER_REGION_PER_FAMILY, 32)
      new HFilePartitioner(splits, fraction)
    }
  }

  protected class HFilePartitioner(splits: Array[Array[Byte]], fraction: Int) extends Partitioner {
    /**
      * 提取rowkey
      */
    def extractKey(n: Any): Array[Byte] = n match {
      case (k: Array[Byte], _) => k // CellKey
      case ((k: Array[Byte], _), _) => k //CellKeyTS
    }

    /**
      * 按每个region n(fraction)个HFile分区
      * 同一region 按rowkey的hashcode来决定写入哪个HFile
      */
    override def getPartition(key: Any): Int = {
      val k = extractKey(key)
      var h = if (fraction > 0) (k.hashCode() & Int.MaxValue) % fraction else 0
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(k, splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }

  protected def getPartitioner(regionLocator: RegionLocator, numFilesPerRegionPerFamily: Int)(implicit config: HBaseConfig) =
    HFilePartitioner(config.get, regionLocator.getStartKeys, numFilesPerRegionPerFamily)

  protected def getPartitionedRdd[C: ClassTag, A: ClassTag](rdd: RDD[(C, A)], kv: KeyValueWrapper[C, A], partitioner: HFilePartitioner)
                                                           (implicit ord: Ordering[C]): RDD[(ImmutableBytesWritable, KeyValue)] = {
    rdd
      .repartitionAndSortWithinPartitions(partitioner)
      .map { case (cell, value) => kv(cell, value) }
  }

  protected def saveAsHFile(rdd: RDD[(ImmutableBytesWritable, KeyValue)], table: Table, regionLocator: RegionLocator, connection: Connection)
                           (implicit config: HBaseConfig): Unit = {
    val conf = config.get
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))

    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    // prepare path for HFiles output
    val fs = FileSystem.get(conf)
    val hFilePath = new Path("/tmp", table.getName.getQualifierAsString /*获取唯一名*/ + "_" + UUID.randomUUID())
    //
    hFilePath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    try {
      rdd
        .saveAsNewAPIHadoopFile(hFilePath.toString, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)

      // prepare HFiles for incremental load
      // set folders permissions read/write/exec for all
      val rwx = new FsPermission("777")

      //不断递归设置文件权限
      def setRecursivePermission(path: Path): Unit = {
        val listFiles = fs.listStatus(path)
        listFiles foreach { f =>
          val p = f.getPath
          fs.setPermission(p, rwx)
          if (f.isDirectory && p.getName != "_tmp") {
            // create a "_tmp" folder that can be used for HFile splitting, so that we can
            // set permissions correctly. This is a workaround for unsecured HBase. It should not
            // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
            // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)
            FileSystem.mkdirs(fs, new Path(p, "_tmp"), rwx)
            setRecursivePermission(p)
          }
        }
      }

      setRecursivePermission(hFilePath)

      //批量导入
      val lih = BulkLoadHFiles.create(conf)
      lih.bulkLoad(table.getName, hFilePath)
    } finally {
      connection.close()

      fs.deleteOnExit(hFilePath)

      // TotalOrderPartitioner.getPartitionFile ==> Get the path to the SequenceFile storing the sorted partition keyset.
      // clean HFileOutputFormat2 stuff
      fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
    }
  }
}

final class HFileRDDSimple[K: Writer, Q: Writer, C: ClassTag, A: ClassTag, V: ClassTag](mapRdd: RDD[(K, Map[Q, A])], ck: GetCellKey[C, A, V], kvf: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
  /**
    * 给定列族
    *
    * RDD ==> rowkey -> column -> value
    */
  def toHBaseBulk(tableNameStr: String, family: String, numFilesPerRegionPerFamily: Int = 1)
                 (implicit config: HBaseConfig, ord: Ordering[C]): Unit = {
    require(numFilesPerRegionPerFamily > 0)
    val wk = implicitly[Writer[K]]
    val wq = implicitly[Writer[Q]]
    val ws = implicitly[Writer[String]]

    val conf = config.get
    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)

    val partitioner = getPartitioner(regionLocator, numFilesPerRegionPerFamily)

    val rdd = mapRdd.flatMap {
      case (k, m) =>
        val keyBytes = wk.write(k)
        m map { case (h, v) => ck((keyBytes, wq.write(h)), v) }
    }

    saveAsHFile(getPartitionedRdd(rdd, kvf(ws.write(family)), partitioner), table, regionLocator, connection)
  }
}

final class HFileRDDFixed[K: Writer, C: ClassTag, A: ClassTag, V: ClassTag](seqRdd: RDD[(K, Seq[A])], ck: GetCellKey[C, A, V], kvf: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
  /**
    * 给定列族和列
    *
    * RDD ==> rowkey -> 一系列value
    */
  def toHBaseBulk[Q: Writer](tableNameStr: String, family: String, headers: Seq[Q], numFilesPerRegionPerFamily: Int = 1)
                            (implicit config: HBaseConfig, ord: Ordering[C]): Unit = {
    require(numFilesPerRegionPerFamily > 0)
    val wk = implicitly[Writer[K]]
    val wq = implicitly[Writer[Q]]
    val ws = implicitly[Writer[String]]

    val conf = config.get
    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)

    val sc = seqRdd.context
    val headersBytes = sc.broadcast(headers map wq.write)
    val partitioner = getPartitioner(regionLocator, numFilesPerRegionPerFamily)

    val rdd = seqRdd.flatMap {
      case (k, v) =>
        val keyBytes = wk.write(k)
        (headersBytes.value zip v) map { case (h, v) => ck((keyBytes, h), v) }
    }

    saveAsHFile(getPartitionedRdd(rdd, kvf(ws.write(family)), partitioner), table, regionLocator, connection)
  }
}

final class HFileRDD[K: Writer, Q: Writer, C: ClassTag, A: ClassTag, V: ClassTag](mapRdd: RDD[(K, Map[String, Map[Q, A]])], ck: GetCellKey[C, A, V], kv: KeyValueWrapperF[C, V]) extends HFileRDDHelper {
  /**
    * RDD ==> rowkey -> column family -> column -> value
    */
  def toHBaseBulk(tableNameStr: String, numFilesPerRegionPerFamily: Int = 1)
                 (implicit config: HBaseConfig, ord: Ordering[C]): Unit = {
    require(numFilesPerRegionPerFamily > 0)
    val wk = implicitly[Writer[K]]
    val wq = implicitly[Writer[Q]]
    val rs = implicitly[Reader[String]]

    val conf = config.get
    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)

    val families = table.getDescriptor.getColumnFamilyNames
    val partitioner = getPartitioner(regionLocator, numFilesPerRegionPerFamily)

    val rdds = for {
      fb <- families.asScala
      f = rs.read(fb)
      rdd = mapRdd
        .collect { case (k, m) if m.contains(f) => (wk.write(k), m(f)) }
        .flatMap {
          case (k, m) =>
            m map { case (h, v) => ck((k, wq.write(h)), v) }
        }
    } yield getPartitionedRdd(rdd, kv(fb), partitioner)

    saveAsHFile(rdds.reduce(_ ++ _), table, regionLocator, connection)
  }
}

/**
  *
  * @param ck 必须带进来, 才能找到Ordering
  * @param kv 必须带进来, 才能找到Ordering
  */
final class HBaseEntityHFileRDD[E <: HBaseEntity, C: ClassTag](entityRdd: RDD[E], ck: GetCellKey[C, Array[Byte], Array[Byte]], kv: KeyValueWrapperF[C, Array[Byte]]) extends HFileRDDHelper {
  /**
    * 将HBaseEntity写入HFile并批量导入
    */
  def toHBaseBulk(tableNameStr: String, numFilesPerRegionPerFamily: Int = 1)
                 (implicit config: HBaseConfig, ord: Ordering[C]): Unit = {
    require(numFilesPerRegionPerFamily > 0)
    val conf = config.get
    val tableName = TableName.valueOf(tableNameStr)
    val connection = ConnectionFactory.createConnection(conf)
    val regionLocator = connection.getRegionLocator(tableName)
    val table = connection.getTable(tableName)
    val families = table.getDescriptor.getColumnFamilyNames
    val partitioner = getPartitioner(regionLocator, numFilesPerRegionPerFamily)

    val rdds = for {
      fb <- families.asScala
      rdd = entityRdd.flatMap(e => {
        val puts = HBaseUtils.convert2Puts(e)
        puts.asScala.flatMap(put => {
          put.getFamilyCellMap.get(fb).asScala.map(cell => {
            (ck((put.getRow, cell.getQualifierArray), cell.getValueArray))
          })
        })
      })
    } yield getPartitionedRdd(rdd, kv(fb), partitioner)

    saveAsHFile(rdds.reduce(_ ++ _), table, regionLocator, connection)
  }
}
