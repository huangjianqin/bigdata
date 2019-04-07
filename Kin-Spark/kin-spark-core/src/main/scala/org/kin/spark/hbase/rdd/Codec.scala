package org.kin.spark.hbase.rdd

/**
  * Created by huangjianqin on 2019/4/7.
  */
trait Reader[T] extends Serializable {
  def read(data: Array[Byte]): T
}

trait Writer[T] extends Serializable {
  def write(data: T): Array[Byte]
}


trait Codec[T] extends Reader[T] with Writer[T]
