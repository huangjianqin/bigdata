package org.kin.spark.hbase.rdd

import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by huangjianqin on 2019/3/31.
  */
trait DefaultWriter {
  implicit val booleanWriter = new Writer[Boolean] {
    def write(data: Boolean): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val byteArrayWriter = new Writer[Array[Byte]] {
    def write(data: Array[Byte]): Array[Byte] = data
  }

  implicit val doubleWriter = new Writer[Double] {
    def write(data: Double): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val floatWriter = new Writer[Float] {
    def write(data: Float): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val intWriter = new Writer[Int] {
    def write(data: Int): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val longWriter = new Writer[Long] {
    def write(data: Long): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val shortWriter = new Writer[Short] {
    def write(data: Short): Array[Byte] = Bytes.toBytes(data)
  }

  implicit val stringWriter = new Writer[String] {
    def write(data: String): Array[Byte] = Bytes.toBytes(data)
  }
}
