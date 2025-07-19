package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object EventTimeWindows {

  val spark = SparkSession.builder()
    .appName("Event Time Windows")
    .master("local[2]")
    .getOrCreate()

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
    .selectExpr("purchase.*")

  def readPurchasesFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def aggregatePurchasesByTumblingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day").as("time")) // tumbling window: sliding duration == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(window(col("time"), "1 day", "1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    * 1) Show the best selling product of every day, +quantity sold.
    * 2) Show the best selling product of every 24 hours, updated every hour.
    */

  def bestSellingProductPerDay() = {
    val purchasesDF = readPurchasesFromFile()

    val bestSelling = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("day"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def bestSellingProductEvery24h() = {
    val purchasesDF = readPurchasesFromFile()

    val bestSelling = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("start"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Multi-Level Time Window Aggregations
  // Create a hierarchical time aggregation that computes purchase statistics at multiple time granularities:
  // minute, hour, and day. The output should show nested aggregations for real-time dashboards.
  def multiLevelTimeAggregations() = {
    val purchasesDF = readPurchasesFromSocket()

    // Minute level aggregations
    val minuteAggs = purchasesDF
      .groupBy(window(col("time"), "1 minute").as("window"))
      .agg(
        sum("quantity").as("totalQuantity"),
        count("id").as("totalTransactions"),
        countDistinct("item").as("uniqueItems"),
        avg("quantity").as("avgQuantity")
      )
      .select(
        lit("minute").as("granularity"),
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("totalQuantity"),
        col("totalTransactions"),
        col("uniqueItems"),
        col("avgQuantity")
      )

    // Hour level aggregations
    val hourAggs = purchasesDF
      .groupBy(window(col("time"), "1 hour").as("window"))
      .agg(
        sum("quantity").as("totalQuantity"),
        count("id").as("totalTransactions"),
        countDistinct("item").as("uniqueItems"),
        avg("quantity").as("avgQuantity")
      )
      .select(
        lit("hour").as("granularity"),
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("totalQuantity"),
        col("totalTransactions"),
        col("uniqueItems"),
        col("avgQuantity")
      )

    // Day level aggregations
    val dayAggs = purchasesDF
      .groupBy(window(col("time"), "1 day").as("window"))
      .agg(
        sum("quantity").as("totalQuantity"),
        count("id").as("totalTransactions"),
        countDistinct("item").as("uniqueItems"),
        avg("quantity").as("avgQuantity")
      )
      .select(
        lit("day").as("granularity"),
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("totalQuantity"),
        col("totalTransactions"),
        col("uniqueItems"),
        col("avgQuantity")
      )

    // Union all levels
    val allLevels = minuteAggs.union(hourAggs).union(dayAggs)
      .orderBy(col("granularity"), col("start"))

    allLevels.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // 2. Session Window Analysis
  // Implement session-based windowing where a session is defined as a sequence of purchases
  // by the same user with gaps no longer than 30 minutes. Use custom logic with foreachBatch.
  def sessionWindowAnalysis() = {
    // First, add a userId to our schema for this exercise
    val purchasesWithUserDF = readPurchasesFromSocket()
      .withColumn("userId", expr("substr(id, 1, 3)")) // Extract first 3 chars as userId

    purchasesWithUserDF.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        import spark.implicits._
        
        if (!batch.isEmpty) {
          // Convert to dataset for easier manipulation
          val purchases = batch.as[(String, java.sql.Timestamp, String, Int, String)]
            .collect()
            .sortBy(_._2.getTime) // Sort by time

          // Group by user and create sessions
          val sessions = purchases.groupBy(_._5) // Group by userId
            .flatMap { case (userId, userPurchases) =>
              val sortedPurchases = userPurchases.sortBy(_._2.getTime)
              var sessions = List.empty[List[(String, java.sql.Timestamp, String, Int, String)]]
              var currentSession = List.empty[(String, java.sql.Timestamp, String, Int, String)]
              
              sortedPurchases.foreach { purchase =>
                if (currentSession.isEmpty) {
                  currentSession = List(purchase)
                } else {
                  val lastPurchaseTime = currentSession.last._2.getTime
                  val currentPurchaseTime = purchase._2.getTime
                  val gapMinutes = (currentPurchaseTime - lastPurchaseTime) / (1000 * 60)
                  
                  if (gapMinutes <= 30) {
                    currentSession = currentSession :+ purchase
                  } else {
                    sessions = sessions :+ currentSession
                    currentSession = List(purchase)
                  }
                }
              }
              
              if (currentSession.nonEmpty) {
                sessions = sessions :+ currentSession
              }
              
              sessions.map { session =>
                val startTime = session.head._2
                val endTime = session.last._2
                val totalQuantity = session.map(_._4).sum
                val uniqueItems = session.map(_._3).distinct.length
                val duration = (endTime.getTime - startTime.getTime) / (1000 * 60) // minutes
                
                (userId, startTime, endTime, totalQuantity, uniqueItems, duration)
              }
            }

          // Create DataFrame from sessions and show
          val sessionDF = spark.createDataFrame(
            spark.sparkContext.parallelize(sessions.toSeq)
          ).toDF("userId", "sessionStart", "sessionEnd", "totalQuantity", "uniqueItems", "durationMinutes")
          
          println(s"--- Batch $batchId: Session Analysis ---")
          sessionDF.show(false)
        }
      }
      .start()
      .awaitTermination()
  }

  // 3. Real-time Percentile Calculations with Event-Time Windows
  // Calculate real-time percentiles (50th, 90th, 95th) of purchase quantities
  // within sliding windows using approximate percentile functions.
  def realTimePercentileAnalysis() = {
    val purchasesDF = readPurchasesFromSocket()

    val percentileStats = purchasesDF
      .groupBy(
        window(col("time"), "10 minutes", "1 minute").as("window"),
        col("item")
      )
      .agg(
        count("quantity").as("totalPurchases"),
        avg("quantity").as("avgQuantity"),
        min("quantity").as("minQuantity"),
        max("quantity").as("maxQuantity"),
        expr("percentile_approx(quantity, 0.5)").as("median"),
        expr("percentile_approx(quantity, 0.90)").as("p90"),
        expr("percentile_approx(quantity, 0.95)").as("p95"),
        expr("percentile_approx(quantity, 0.99)").as("p99")
      )
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("item"),
        col("totalPurchases"),
        col("avgQuantity"),
        col("minQuantity"),
        col("maxQuantity"),
        col("median"),
        col("p90"),
        col("p95"),
        col("p99")
      )
      .filter(col("totalPurchases") >= 5) // Only show windows with sufficient data

    percentileStats.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // 4. Dynamic Window Size Based on Data Velocity
  // Implement adaptive windowing where window size changes based on the rate of incoming data.
  // High velocity periods use smaller windows, low velocity periods use larger windows.
  def adaptiveWindowSizing() = {
    val purchasesDF = readPurchasesFromSocket()

    purchasesDF.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        import spark.implicits._
        
        if (!batch.isEmpty) {
          val currentTime = System.currentTimeMillis()
          val batchSize = batch.count()
          val recordsPerSecond = batchSize // Simplified rate calculation
          
          // Determine window size based on velocity
          val windowSize = recordsPerSecond match {
            case rate if rate > 100 => "30 seconds"   // High velocity
            case rate if rate > 50  => "1 minute"     // Medium velocity
            case rate if rate > 10  => "5 minutes"    // Low velocity
            case _                  => "15 minutes"   // Very low velocity
          }
          
          println(s"Batch $batchId: Rate = $recordsPerSecond records/batch, Using window = $windowSize")
          
          // Apply the determined window size
          val windowedAgg = batch
            .groupBy(window(col("time"), windowSize).as("window"))
            .agg(
              sum("quantity").as("totalQuantity"),
              count("id").as("totalTransactions"),
              avg("quantity").as("avgQuantity")
            )
            .select(
              col("window").getField("start").as("start"),
              col("window").getField("end").as("end"),
              col("totalQuantity"),
              col("totalTransactions"),
              col("avgQuantity"),
              lit(windowSize).as("windowSize"),
              lit(recordsPerSecond).as("dataVelocity")
            )
          
          windowedAgg.show(false)
        }
      }
      .start()
      .awaitTermination()
  }

  /*
    For window functions, windows start at Jan 1 1970, 12 AM GMT
   */

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and start a socket session: nc -lk 12345
    // Then send JSON data like: {"id":"001","time":"2023-01-01T10:00:00.000Z","item":"laptop","quantity":2}
    // multiLevelTimeAggregations()
    // sessionWindowAnalysis()
    // realTimePercentileAnalysis()
    adaptiveWindowSizing()
  }
}
