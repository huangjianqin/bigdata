package org.kin.spark.hbase.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}

/**
  * Created by huangjianqin on 2019/4/7.
  */
class HBaseConfig(defaults: Configuration) extends Serializable {
  def get: Configuration = HBaseConfiguration.create(defaults)
}

object HBaseConfig {
  def apply(conf: Configuration): HBaseConfig = new HBaseConfig(conf)

  def apply(options: (String, String)*): HBaseConfig = {
    val conf = HBaseConfiguration.create

    for ((key, value) <- options) {
      conf.set(key, value)
    }

    apply(conf)
  }

  def apply(rootdir: String, quorum: String): HBaseConfig = apply(
    HConstants.HBASE_DIR -> rootdir,
    HConstants.ZOOKEEPER_QUORUM -> quorum)
}