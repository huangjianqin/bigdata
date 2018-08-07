package org.kin.bigdata.scala.mongo

import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.{BSONDocumentReader, BSONDocumentWriter, Macros, document}

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}

/**
  * Created by huangjianqin on 2017/12/7.
  * asynchronous non-blocking 底层由Akka支持，高并发，与Play无缝对接
  */
object ReactiveMongoTest {
  implicit def personWriter: BSONDocumentWriter[Person] = Macros.writer[Person]
  implicit def personReader: BSONDocumentReader[Person] = Macros.reader[Person]
  // or provide a custom one

  case class Person(firstName: String, lastName: String, age: Int)

  def main(args: Array[String]): Unit = {
    var uri = "mongodb://localhost:27017/hjq?"

    import scala.concurrent.ExecutionContext.Implicits.global

    // Connect to the database: Must be done only once per application
    val driver = new MongoDriver()

    val parsedUri = MongoConnection.parseURI(uri)
    val connection = parsedUri.map{
      var conOpts = MongoConnectionOptions(/* connection options */)
//      driver.connection(List(uri), conOpts = conOpts)
      driver.connection(_)
    }

    // Database and collections: Get references
    val futureConnection = Future.fromTry(connection)
    def db: Future[DefaultDB] = futureConnection.flatMap(_.database("hjq"))
    def personCollection: Future[BSONCollection] = db.map(_.collection("person"))

    //操作函数
    def createPerson(person: Person): Future[Unit] =
      personCollection.flatMap(_.insert(person).map(_ => {}))

    def updatePerson(person: Person): Future[Int] = {
      val selector = document(
        "firstName" -> person.firstName,
        "lastName" -> person.lastName
      )

      // Update the matching person
      //设置multi=true只适用于参数是命令的时候，不可以是case class
      personCollection.flatMap(_.update(selector, person).map(_.n))
    }

    val failOnErrorPrintln = Cursor.FailOnError[List[Person]]{
      (a, throwable) =>
        println(throwable)
    }

    def findPerson: Future[List[Person]] = personCollection.flatMap(
        _.find(document).cursor[Person]().collect[List](100, failOnErrorPrintln))

    def findPersonByAge(age: Int): Future[List[Person]] =
      personCollection.flatMap(_.find(document("age" -> age)). // query builder
        cursor[Person]().collect[List](100, failOnErrorPrintln)) // collect using the result cursor
    // ... deserializes the document using personReader

    def closeConnection = {
      val closingConnection = futureConnection.map(_.close())
      closingConnection.onComplete{
        case result =>{
          println(result)
          driver.close()
        }
      }
    }

    //操作Future
    findPerson.onComplete({
      case Success(list) => {
        println(list.size)
        list.foreach(println)
        closeConnection
      }
      case Failure(e) => println(e)
        closeConnection
    })

  }
}
