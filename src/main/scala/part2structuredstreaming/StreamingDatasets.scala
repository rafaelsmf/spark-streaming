package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {

  val spark = SparkSession.builder()
    .appName("Streaming Datasets")
    .master("local[2]")
    .getOrCreate()

  // include encoders for DF -> DS transformations
  import spark.implicits._

  def readCars(): Dataset[Car] = {
    // useful for DF -> DS transformations
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // DF with single string column "value"
      .select(from_json(col("value"), carsSchema).as("car")) // composite column (struct)
      .selectExpr("car.*") // DF with multiple columns
      .as[Car] // encoder can be passed implicitly with spark.implicits
  }

  def showCarNames() = {
    val carsDS: Dataset[Car] = readCars()

    // transformations here
    val carNamesDF: DataFrame = carsDS.select(col("Name")) // DF

    // collection transformations maintain type info
    val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    *
    * 1) Count how many POWERFUL cars we have in the DS (HP > 140)
    * 2) Average HP for the entire dataset
    *   (use the complete output mode)
    * 3) Count the cars by origin
    */

  def ex1() = {
    val carsDS = readCars()
    carsDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def ex2() = {
    val carsDS = readCars()

    carsDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def ex3() = {
    val carsDS = readCars()

    val carCountByOrigin = carsDS.groupBy(col("Origin")).count() // option 1
    val carCountByOriginAlt = carsDS.groupByKey(car => car.Origin).count() // option 2 with the Dataset API

    carCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 4. Implement a running count of cars per origin, and detect if any origin has more than 5 cars.
  //    Use a stateful transformation with mapGroupsWithState.
  case class CarCountState(count: Long)
  case class CarCountResult(origin: String, count: Long, alert: Boolean)

  def countCarsWithState(): Unit = {
    val carsDS = readCars()

    val carCountByOrigin = carsDS
      .groupByKey(_.Origin)
      .mapGroupsWithState[CarCountState, CarCountResult] {
        case (origin: String, cars: Iterator[Car], state: GroupState[CarCountState]) =>
          val previousCount = state.getOption.map(_.count).getOrElse(0L)
          val newCount = previousCount + cars.length
          state.update(CarCountState(newCount))
          CarCountResult(origin, newCount, newCount > 5)
      }

    carCountByOrigin.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // 5. Calculate the average weight of cars per cylinder count.
  //    Handle potential nulls in Horsepower and Weight_in_lbs gracefully.
  def averageWeightPerCylinders(): Unit = {
    val carsDS = readCars()

    val avgWeightDS = carsDS
      .filter(car => car.Cylinders.isDefined && car.Weight_in_lbs.isDefined)
      .groupByKey(_.Cylinders.get)
      .agg(
        avg("Weight_in_lbs").as[Double],
        count("*").as[Long]
      )
      .map {
        case (cylinders, avgWeight, count) =>
          s"Cylinders: $cylinders, AvgWeight: $avgWeight, Count: $count"
      }

    avgWeightDS.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  // 6. Detect "gas guzzlers" - cars with low Miles_per_Gallon for their Horsepower.
  //    A car is a gas guzzler if its MPG is in the bottom 10% for its horsepower bracket.
  //    Horsepower brackets: 0-100, 101-150, 151-200, 200+
  def detectGasGuzzlers(): Unit = {
    val carsDS = readCars()

    val carsWithHPBracket = carsDS
      .filter(c => c.Horsepower.isDefined && c.Miles_per_Gallon.isDefined)
      .map { car =>
        val hp = car.Horsepower.get
        val bracket = hp match {
          case _ if hp <= 100 => "0-100"
          case _ if hp <= 150 => "101-150"
          case _ if hp <= 200 => "151-200"
          case _ => "200+"
        }
        (bracket, car)
      }

    carsWithHPBracket.toDF("hp_bracket", "car")
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          val percentile = 0.1 // bottom 10%
          val windowSpec = org.apache.spark.sql.expressions.Window.partitionBy("hp_bracket")

          val carsWithPercentile = batchDF
            .withColumn("mpg_percentile", percent_rank().over(windowSpec.orderBy("car.Miles_per_Gallon")))

          val gasGuzzlers = carsWithPercentile
            .filter(col("mpg_percentile") <= percentile)
            .select("car.Name", "car.Miles_per_Gallon", "car.Horsepower", "hp_bracket")

          println(s"--- Batch $batchId: Gas Guzzlers ---")
          gasGuzzlers.show()
        }
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it.
    // Start a netcat session: nc -lk 12345
    // and paste car JSON data, e.g.:
    // {"Name":"chevrolet chevelle malibu","Miles_per_Gallon":18,"Cylinders":8,"Displacement":307,"Horsepower":130,"Weight_in_lbs":3504,"Acceleration":12,"Year":"1970-01-01","Origin":"USA"}
    // {"Name":"buick skylark 320","Miles_per_Gallon":15,"Cylinders":8,"Displacement":350,"Horsepower":165,"Weight_in_lbs":3693,"Acceleration":11.5,"Year":"1970-01-01","Origin":"USA"}

    countCarsWithState()
  }
}
