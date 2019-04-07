package org.kin.spark.hbase.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.kin.spark.hbase.rdd.{HBaseConfig, Writer}

import scala.reflect.ClassTag

/**
  * Created by huangjianqin on 2019/4/7.
  */
trait HBaseUtils {
  protected[hbase] def createJob(table: String, conf: Configuration): Job = {
    conf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance(conf, this.getClass.getName.split('$')(0))
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])
    job
  }

  object HBaseAdmin {
    def apply()(implicit config: HBaseConfig) = new HBaseAdmin(ConnectionFactory.createConnection(config.get))
  }

  class HBaseAdmin(connection: Connection) {

    def close() = connection.close()

    def tableExists(tableName: String, family: String)(implicit wq: Writer[String]): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        val families = admin.getTableDescriptor(table).getFamiliesKeys
        require(families.contains(wq.write(family)), s"Table [$table] exists but column family [$family] is missing")
        true
      } else false
    }

    def tableExists(tableName: String, families: Set[String])(implicit wq: Writer[String]): Boolean = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table)) {
        val tfamilies = admin.getTableDescriptor(table).getFamiliesKeys
        for (family <- families)
          require(tfamilies.contains(wq.write(family)), s"Table [$table] exists but column family [$family] is missing")
        true
      } else false
    }

    /**
      *
      */
    def snapshot(tableName: String): HBaseAdmin = {
      val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
      val suffix = sdf.format(Calendar.getInstance().getTime)
      snapshot(tableName, s"${tableName}_$suffix")
      this
    }

    /**
      *
      */
    def snapshot(tableName: String, snapshotName: String): HBaseAdmin = {
      val admin = connection.getAdmin
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      admin.snapshot(snapshotName, tableDescriptor.getTableName)
      this
    }

    /**
      *
      * @param splitKeys ??
      */
    def createTable[K](tableName: String, families: Set[String], splitKeys: Seq[K])(implicit wk: Writer[K], wq: Writer[String]): HBaseAdmin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (!admin.isTableAvailable(table)) {
        val tableDescriptor = new HTableDescriptor(table)
        families foreach { f => tableDescriptor.addFamily(new HColumnDescriptor(wq.write(f))) }
        if (splitKeys.isEmpty)
          admin.createTable(tableDescriptor)
        else {
          val splitKeysBytes = splitKeys.map(wk.write).toArray
          admin.createTable(tableDescriptor, splitKeysBytes)
        }
      }
      this
    }

    def createTable(tableName: String, families: Set[String]): HBaseAdmin =
      createTable[String](tableName, families, Nil)

    def createTable(tableName: String, families: String*): HBaseAdmin =
      createTable[String](tableName, families.toSet, Nil)

    def createTable[K: Writer](tableName: String, family: String, splitKeys: Seq[K]): HBaseAdmin =
      createTable(tableName, Set(family), splitKeys)

    def disableTable(tableName: String): HBaseAdmin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.disableTable(table)
      this
    }

    def deleteTable(tableName: String): HBaseAdmin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.deleteTable(table)
      this
    }

    /**
      *
      */
    def truncateTable(tableName: String, preserveSplits: Boolean): HBaseAdmin = {
      val admin = connection.getAdmin
      val table = TableName.valueOf(tableName)
      if (admin.tableExists(table))
        admin.truncateTable(table, preserveSplits)
      this
    }
  }

  /**
    *
    */
  def computeSplits[K: ClassTag](rdd: RDD[K], regionsCount: Int)(implicit ord: Ordering[K]): Seq[K] = {
    rdd.sortBy(s => s, numPartitions = regionsCount)
      .mapPartitions(_.take(1))
      .collect().toList.tail
  }
}

