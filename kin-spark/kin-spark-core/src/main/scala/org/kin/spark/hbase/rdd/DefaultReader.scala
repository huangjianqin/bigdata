package org.kin.spark.hbase.rdd

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by huangjianqin on 2019/3/31.
  */
trait DefaultReader {
  implicit val booleanReader = new Reader[Boolean] {
    def read(data: Array[Byte]): Boolean = Bytes.toBoolean(data)
  }

  implicit val byteArrayReader = new Reader[Array[Byte]] {
    def read(data: Array[Byte]): Array[Byte] = data
  }

  implicit val doubleReader = new Reader[Double] {
    def read(data: Array[Byte]): Double = Bytes.toDouble(data)
  }

  implicit val floatReader = new Reader[Float] {
    def read(data: Array[Byte]): Float = Bytes.toFloat(data)
  }

  implicit val intReader = new Reader[Int] {
    def read(data: Array[Byte]): Int = Bytes.toInt(data)
  }

  implicit val longReader = new Reader[Long] {
    def read(data: Array[Byte]): Long = Bytes.toLong(data)
  }

  implicit val shortReader = new Reader[Short] {
    def read(data: Array[Byte]): Short = Bytes.toShort(data)
  }

  implicit val stringReader = new Reader[String] {
    def read(data: Array[Byte]): String = Bytes.toString(data)
  }
}
