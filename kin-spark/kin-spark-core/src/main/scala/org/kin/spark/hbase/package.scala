package org.kin.spark

import org.kin.spark.hbase.rdd.{HFileSupport, _}
import org.kin.spark.hbase.util.HBaseUtils

/**
  * Created by huangjianqin on 2019/4/7.
  *
  * 假定列族是string
  */
package object hbase extends DefaultReader
  with DefaultWriter
  with HBaseReadSupport
  with HBaseWriteSupport
  with HFileSupport
  with HBaseUtils {

}
