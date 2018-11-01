package org.kin.bigdata.spark.mongo

import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

/**
  * Created by 健勤 on 2017/6/11.
  */
object MongoSparkTest {
  def main(args: Array[String]) {
    val conf =
      new SparkConf()
        .setAppName("Mongo-Spark-Test")
        .setMaster("spark://192.168.1.102:7077")
        //        .setMaster("local[1]")
        .setJars(Seq("out/artifacts/bigdata_project_jar/bigdata-project.jar"))
        .set("spark.mongodb.input.uri", "mongodb://192.168.1.102/test.c1?readPreference=primaryPreferred")
        .set("spark.mongodb.output.uri", "mongodb://192.168.1.102/test.c1")

    val sc = new SparkContext(conf)
    val bsonRDD = sc.parallelize(Range(10, 20)).map { i =>
      (i, i)
    }.map { case (x, y) =>
      Document.parse(s"{a: $x, b: $y}")
    }
    //保存BSON至SparkConf配置的mongodb数据库中
    MongoSpark.save(bsonRDD)

    //通过 WriteConfig自定义mongodb数据库配置
    //可以通过参数或Map来构建 WriteConfig
    //下面方法中第一个参数使用Map构建的 WriteConfig,第二个方法是传入默认的 WriteConfig(此处是SparkConf定义的配置)
    //    MongoSpark.save(bsonRDD, WriteConfig(Map("collection" -> "c2", "writeConcern.w" -> "majority"), Some(WriteConfig(sc))))

    //使用了隐式转换
    //    bsonRDD.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://139.199.185.84/test.c3")))

    //使用SparkConf配置
    //    val loadedRDD = MongoSpark.load(sc)

    //使用ReadConfig自定义mongodb配置
    //    val loadedRDD = MongoSpark.load(sc, ReadConfig(Map("uri" -> "mongodb://139.199.185.84/test.c3?readPreference=primaryPreferred")))

    //使用隐式转换
    //    import com.mongodb.spark._
    //    val loadedRDD = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://139.199.185.84/test.c3?readPreference=primaryPreferred")))
    //
    //    loadedRDD.foreach{document =>
    //      println(document.toJson)
    //    }

    //使用aggregation pipeline会比直接使用RDD.filter更高效
    //同时aggregation pipeline提供了更多过滤规则,检查null,如果RDD类型不是Document,那么会抛出java.lang.NullPointerException
    //    import com.mongodb.spark._
    //    val loadedRDD = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://139.199.185.84/test.c3?readPreference=primaryPreferred")))
    //    val aggregatedRDD = loadedRDD.withPipeline(Seq(Document.parse("{ $match: { a : { $gt : 15 } } }")))
    //
    //    aggregatedRDD.foreach{document =>
    //      println(document.toJson)
    //    }

    //使用下面的api可以高度自定义MongoSpark
    //    MongoSpark.builder()

    //利用SparkConf的mongodb配置加载到Spark SQL转换成DataFrame
    //    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    //    val df1 = MongoSpark.load(sparkSession)
    //    df1.registerTempTable("test")
    //    sparkSession.sql("select * from test where test.a != test.b").foreach(println)
    //
    //    利用隐式转换读取mongodb集合
    //    val df1 = sparkSession.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://139.199.185.84/test.c3?readPreference=primaryPreferred")))
    //    df1.registerTempTable("test")
    //    sparkSession.sql("select * from test where test.a > 15").foreach(println)
    //
    //    利用sqlContext.read
    //    val df1 = sparkSession.read.format("com.mongodb.spark.sql").load()
    //    df1.registerTempTable("test")
    //    sparkSession.sql("select * from test where test.a != test.b").foreach(println)
    //
    //    val readConfig = ReadConfig(Map("uri" -> "mongodb://139.199.185.84/test.c3?readPreference=primaryPreferred"))
    //    val df1 = sparkSession.read.mongo(readConfig)
    //    df1.registerTempTable("test")
    //    sparkSession.sql("select * from test where test.a > 15").foreach(println)
    //
    //    val df1 = sparkSession.read.format("com.mongodb.spark.sql").options(readConfig.asOptions).load()
    //    df1.registerTempTable("test")
    //    sparkSession.sql("select * from test where test.a > 15").foreach(println)
    //
    //    使用filter,在底层SQLContext是使用aggregation pipeline来过滤数据
    //    val df1 = sparkSession.read.format("com.mongodb.spark.sql").load()
    //    df1.filter(df1("a") === df1("b")).show()
    //
    //    默认情况下,会使用mongo集合document的schema作为dataframe的schema
    //    使用case类的话可以自定义dataframe的schema,而且只会读取这些field
    //    val df1 = MongoSpark.load[A](sparkSession)
    //    df1.printSchema()
    //
    //    转换成dataset
    //    val dataset = df1.as[A]
    //    dataset.show()
    //
    //    RDD[document]转换成DataFrame或Dataset
    //    val rdd = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://139.199.185.84/test.c3?readPreference=primaryPreferred")))
    //    val df1 = rdd.toDF()
    //    df1.printSchema()
    //    val df2 = rdd.toDF[A]()
    //    df2.printSchema()
    //    val ds1 = rdd.toDS[A]()
    //    ds1.show()
    //
    //    保存至MongoDB
    //    MongoSpark.save(df1.filter(df1("a") === 19).write.option("collection", "c4").mode("overwrite"))
    //    df1.filter(df1("a") === 18).write.option("collection", "c4").mode("overwrite").mongo()
    //    df1.filter(df1("a") === 19).write.option("collection", "c4").format("com.mongodb.spark.sql").mode("append").save()

    //    sparkSession.stop()
    sc.stop()
  }
}

case class A(a: String) {}