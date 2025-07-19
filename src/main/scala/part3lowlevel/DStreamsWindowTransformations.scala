package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object DStreamsWindowTransformations {

  val spark = SparkSession.builder()
    .appName("DStream Window Transformations")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readLines(): DStream[String] = ssc.socketTextStream("localhost", 12345)

  /*
    window = keep all the values emitted between now and X time back
    window interval updated with every batch
    window interval must be a multiple of the batch interval
   */
  def linesByWindow() = readLines().window(Seconds(10))

  /*
    first arg = window duration
    second arg = sliding duration
    both args need to be a multiple of the original batch duration
   */
  def linesBySlidingWindow() = readLines().window(Seconds(10), Seconds(5))

  // count the number of elements over a window
  def countLinesByWindow() = readLines().countByWindow(Minutes(60), Seconds(30))

  // aggregate data in a different way over a window
  def sumAllTextByWindow() = readLines().map(_.length).window(Seconds(10), Seconds(5)).reduce(_ + _)

  // identical
  def sumAllTextByWindowAlt() = readLines().map(_.length).reduceByWindow(_ + _, Seconds(10), Seconds(5))

  // tumbling windows
  def linesByTumblingWindow() = readLines().window(Seconds(10), Seconds(10))  // batch of batches

  def computeWordOccurrencesByWindow() = {
    // for reduce by key and window you need a checkpoint directory set
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b, // reduction function
        (a, b) => a - b, // "inverse" function
        Seconds(60), // window duration
        Seconds(30)  // sliding duration
      )
  }

  /**
    * Exercise.
    * Word longer than 10 chars => $2
    * Every other word => $0
    *
    * Input text into the terminal => money made over the past 30 seconds, updated every 10 seconds.
    * - use window
    * - use countByWindow
    * - use reduceByWindow
    * - use reduceByKeyAndWindow
    */

  val moneyPerExpensiveWord = 2

  def showMeTheMoney() = readLines()
    .flatMap(line => line.split(" "))
    .filter(_.length >= 10)
    .map(_ => moneyPerExpensiveWord)
    .reduce(_ + _)
    .window(Seconds(30), Seconds(10))
    .reduce(_ + _)

  def showMeTheMoney2() = readLines()
    .flatMap(_.split(" "))
    .filter(_.length >= 10)
    .countByWindow(Seconds(30), Seconds(10))
    .map(_ * moneyPerExpensiveWord)

  def showMeTheMoney3() = readLines()
    .flatMap(line => line.split(" "))
    .filter(_.length >= 10)
    .map(_ => moneyPerExpensiveWord)
    .reduceByWindow(_ + _, Seconds(30), Seconds(10))

  def showMeTheMoney4() = {
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(line => line.split(" "))
      .filter(_.length >= 10)
      .map { word =>
        if (word.length >= 10) ("expensive", 2)
        else ("cheap", 1)
      }
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(10))
  }


  /**
    * Extra Exercises
    */

  // 1. Real-time Anomaly Detection in a sliding window.
  //    Stream of sensor data: "sensorId,value".
  //    Over a 5-minute window sliding every 30 seconds, for each sensor, calculate the mean and standard deviation.
  //    An anomaly is a value that is more than 3 standard deviations from the mean.
  //    Output: (sensorId, anomalousValue, mean, stddev)
  def anomalyDetection(): Unit = {
    ssc.checkpoint("checkpoints")

    val sensorStream = readLines().map { line =>
      val t = line.split(","); (t(0), t(1).toDouble)
    }

    // State: (count, sum, sum of squares)
    type SensorState = (Long, Double, Double)

    val updateState = (values: Seq[Double], state: Option[SensorState]) => {
      if (values.isEmpty) {
        state // No new data, return old state
      } else {
        val (count, sum, sumSq) = state.getOrElse((0L, 0.0, 0.0))
        val newCount = count + values.length
        val newSum = sum + values.sum
        val newSumSq = sumSq + values.map(x => x * x).sum
        Some((newCount, newSum, newSumSq))
      }
    }

    val sensorStats = sensorStream.groupByKey().updateStateByKey(updateState)

    // This is complex with updateStateByKey alone. A better approach is to use reduceByKeyAndWindow with an inverse function.
    // State: (count, sum, sum of squares)
    val initialVal = (0L, 0.0, 0.0)
    val reduceFunc = (acc: SensorState, value: Double) => (acc._1 + 1, acc._2 + value, acc._3 + value * value)
    val inverseFunc = (acc: SensorState, value: Double) => (acc._1 - 1, acc._2 - value, acc._3 - value * value)

    val windowedStats = sensorStream
      .window(Seconds(300), Seconds(30)) // 5 min window, 30 sec slide
      .map(v => (v._1, (1L, v._2, v._2 * v._2))) // (sensorId, (1, value, value^2))
      .reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3) }

    // We can't easily join the raw stream with the windowed aggregate in DStreams.
    // A common pattern is to do the check within a transform operation.
    val anomalyDStream = sensorStream.window(Seconds(30), Seconds(30)).transform { rdd =>
      val statsRDD = windowedStats.compute(rdd.sparkContext.getCheckpointDir.nonEmpty).getOrElse(spark.sparkContext.emptyRDD)
      
      val statsMap = statsRDD.collectAsMap()
      val broadcastStats = spark.sparkContext.broadcast(statsMap)

      rdd.mapPartitions { iter =>
        val stats = broadcastStats.value
        iter.flatMap { case (sensorId, value) =>
          stats.get(sensorId) match {
            case Some((count, sum, sumSq)) if count > 1 =>
              val mean = sum / count
              val stddev = Math.sqrt((sumSq / count) - (mean * mean))
              if (Math.abs(value - mean) > 3 * stddev) {
                Some((sensorId, value, mean, stddev))
              } else {
                None
              }
            case _ => None
          }
        }
      }
    }
    anomalyDStream.print()
  }

  // 2. Top-K Trending Items in a Sliding Window
  //    Stream of hashtags from social media.
  //    Find the top 5 trending hashtags over the last 10 minutes, updated every minute.
  def topKHashtags(): Unit = {
    ssc.checkpoint("checkpoints")

    val hashtags = readLines().flatMap(_.split(" ")).filter(_.startsWith("#"))

    val hashtagCounts = hashtags.map((_, 1)).reduceByKeyAndWindow(
      _ + _, // add new counts
      _ - _, // subtract old counts
      Seconds(600), // 10 min window
      Seconds(60)   // 1 min slide
    )

    val topHashtags = hashtagCounts.transform { rdd =>
      rdd.sortBy(_._2, ascending = false)
         .zipWithIndex() // prepare for ranking
         .filter(_._2 < 5) // take top 5
         .map(_._1)
    }

    topHashtags.print()
  }

  // 3. Alerting on sustained high-load
  //    A stream of server load metrics: "serverId,load".
  //    Fire an alert if a server's average load over the last 2 minutes has been > 0.8 for at least 3 consecutive windows (updated every 30s).
  def sustainedHighLoadAlert(): Unit = {
    ssc.checkpoint("checkpoints")

    val metricsStream = readLines().map { line =>
      val t = line.split(","); (t(0), t(1).toDouble)
    }

    val avgLoadByServer = metricsStream
      .mapValues(load => (load, 1))
      .reduceByKeyAndWindow(
        { (a, b) => (a._1 + b._1, a._2 + b._2) },
        { (a, b) => (a._1 - b._1, a._2 - b._2) },
        Seconds(120), // 2 min window
        Seconds(30)   // 30s slide
      )
      .mapValues { case (totalLoad, count) => if (count > 0) totalLoad / count else 0.0 }

    val highLoadStream = avgLoadByServer.filter(_._2 > 0.8)

    // State: consecutive high-load count
    val alertStateUpdate = (serverId: String, avgLoad: Option[Double], state: State[Int]) => {
      if (avgLoad.isDefined) {
        val newCount = state.getOption.getOrElse(0) + 1
        state.update(newCount)
        if (newCount >= 3) {
          Some((serverId, avgLoad.get, newCount)) // Alert!
        } else {
          None
        }
      } else {
        state.remove() // Reset count if load drops
        None
      }
    }

    val alerts = highLoadStream.mapWithState(StateSpec.function(alertStateUpdate))

    alerts.print()
  }


  def main(args: Array[String]): Unit = {
    computeWordOccurrencesByWindow().print()
    // To run an exercise, uncomment it and start a netcat session: nc -lk 12345
    // For anomalyDetection: sensorA,50.0 (and many other values for sensorA)
    // anomalyDetection()

    // For topKHashtags: #spark #scala #bigdata #spark #streaming #kafka #spark
    // topKHashtags()

    // For sustainedHighLoadAlert: server1,0.9 (repetidamente)
    sustainedHighLoadAlert()

    ssc.start()
    ssc.awaitTermination()
  }
}
