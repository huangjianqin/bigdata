package org.kin.bigdata.flink.stream

import java.beans.Transient
import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger

import scala.util.Random

/**
  * Created by huangjianqin on 2019/1/7.
  */
object TopSpeedWindow {
  case class CarEvent(carId: Int, speed: Int, distance: Double, time: Long)

  val numOfCars = 1
  val evictionSec = 10
  val triggerMeters = 500d

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val cars = env.addSource(new SourceFunction[CarEvent]() {

      val speeds = Array.fill[Integer](numOfCars)(50)
      val distances = Array.fill[Double](numOfCars)(0d)
      @Transient lazy val rand = new Random()

      var isRunning:Boolean = true

      override def run(ctx: SourceContext[CarEvent]) = {
        while (isRunning) {
          Thread.sleep(100)

          for (carId <- 0 until numOfCars) {
            if (rand.nextBoolean) speeds(carId) = Math.min(100, speeds(carId) + 5)
            else speeds(carId) = Math.max(0, speeds(carId) - 5)

            distances(carId) += speeds(carId) / 3.6d
            val record = CarEvent(carId, speeds(carId),
              distances(carId), System.currentTimeMillis)
            ctx.collect(record)
          }
        }
      }

      override def cancel(): Unit = isRunning = false
    })

    val topSeed = cars
      .assignAscendingTimestamps( _.time )
      .keyBy("carId")
      .window(GlobalWindows.create)
      .evictor(TimeEvictor.of(Time.of(evictionSec * 1000, TimeUnit.MILLISECONDS)))
      .trigger(DeltaTrigger.of(triggerMeters, new DeltaFunction[CarEvent] {
        def getDelta(oldSp: CarEvent, newSp: CarEvent): Double = newSp.distance - oldSp.distance
      }, cars.getType().createSerializer(env.getConfig)))
      //      .window(Time.of(evictionSec * 1000, (car : CarEvent) => car.time))
      //      .every(Delta.of[CarEvent](triggerMeters,
      //          (oldSp,newSp) => newSp.distance-oldSp.distance, CarEvent(0,0,0,0)))
      .maxBy("speed")

    topSeed.print()

    env.execute("TopSpeedWindow")
  }

  def parseMap(line : String): (Int, Int, Double, Long) = {
    val record = line.substring(1, line.length - 1).split(",")
    (record(0).toInt, record(1).toInt, record(2).toDouble, record(3).toLong)
  }
}
