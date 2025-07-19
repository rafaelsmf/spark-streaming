package part6advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration._

object Watermarks {
  val spark = SparkSession.builder()
    .appName("Late Data with Watermarks")
    .master("local[2]")
    .getOrCreate()

  // 3000,blue

  import spark.implicits._

  def debugQuery(query: StreamingQuery) = {
    // useful skill for debugging
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime =
          if (query.lastProgress == null) "[]"
          else query.lastProgress.eventTime.toString

        println(s"$i: $queryEventTime")
      }
    }).start()
  }

  def testWatermark() = {
    val dataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val data = tokens(1)

        (timestamp, data)
      }
      .toDF("created", "color")

    val watermarkedDF = dataDF
      .withWatermark("created", "2 seconds") // adding a 2 second watermark
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count()
      .selectExpr("window.*", "color", "count")

    /*
      A 2 second watermark means
      - a window will only be considered until the watermark surpasses the window end
      - an element/a row/a record will be considered if AFTER the watermark
     */

    val query = watermarkedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    debugQuery(query)
    query.awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Multi-Level Watermark Strategy
  // Implement different watermark strategies for different types of data streams.
  // Critical data gets a short watermark (low latency), while less critical data gets a longer watermark.
  def multiLevelWatermarkStrategy() = {
    val rawDataDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val dataType = tokens(1)
        val value = tokens(2)

        (timestamp, dataType, value)
      }
      .toDF("eventTime", "dataType", "value")

    // Split streams by data type
    val criticalData = rawDataDF.filter(col("dataType") === "critical")
      .withWatermark("eventTime", "1 second") // Short watermark for critical data
      .groupBy(window(col("eventTime"), "5 seconds"), col("dataType"))
      .count()
      .selectExpr("window.*", "dataType", "count", "'critical_stream' as source")

    val normalData = rawDataDF.filter(col("dataType") === "normal")
      .withWatermark("eventTime", "10 seconds") // Longer watermark for normal data
      .groupBy(window(col("eventTime"), "10 seconds"), col("dataType"))
      .count()
      .selectExpr("window.*", "dataType", "count", "'normal_stream' as source")

    val lowPriorityData = rawDataDF.filter(col("dataType") === "batch")
      .withWatermark("eventTime", "30 seconds") // Even longer watermark for batch processing
      .groupBy(window(col("eventTime"), "30 seconds"), col("dataType"))
      .count()
      .selectExpr("window.*", "dataType", "count", "'batch_stream' as source")

    // Union all streams
    val combinedStream = criticalData.union(normalData).union(lowPriorityData)

    val query = combinedStream.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()

    debugQuery(query)
    query.awaitTermination()
  }

  // 2. Watermark Impact Analysis
  // Analyze how different watermark settings affect data completeness and latency.
  // Track late data statistics and processing delays.
  def watermarkImpactAnalysis() = {
    val dataWithMetricsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val eventTime = new Timestamp(tokens(0).toLong)
        val data = tokens(1)
        val processingTime = new Timestamp(System.currentTimeMillis())

        (eventTime, data, processingTime)
      }
      .toDF("eventTime", "data", "processingTime")
      .withColumn("latencyMs", 
        (unix_timestamp(col("processingTime")) - unix_timestamp(col("eventTime"))) * 1000)

    // Test different watermark settings
    val shortWatermark = dataWithMetricsDF
      .withWatermark("eventTime", "2 seconds")
      .groupBy(window(col("eventTime"), "5 seconds"))
      .agg(
        count("*").as("recordCount"),
        avg("latencyMs").as("avgLatency"),
        max("latencyMs").as("maxLatency"),
        countDistinct("data").as("uniqueValues")
      )
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("recordCount"),
        col("avgLatency"),
        col("maxLatency"),
        col("uniqueValues"),
        lit("2_seconds").as("watermarkSetting")
      )

    val longWatermark = dataWithMetricsDF
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window(col("eventTime"), "5 seconds"))
      .agg(
        count("*").as("recordCount"),
        avg("latencyMs").as("avgLatency"),
        max("latencyMs").as("maxLatency"),
        countDistinct("data").as("uniqueValues")
      )
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("recordCount"),
        col("avgLatency"),
        col("maxLatency"),
        col("uniqueValues"),
        lit("10_seconds").as("watermarkSetting")
      )

    val combinedAnalysis = shortWatermark.union(longWatermark)

    val query = combinedAnalysis.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()

    debugQuery(query)
    query.awaitTermination()
  }

  // 3. Late Data Recovery with Side Output
  // Capture late-arriving data that falls outside the watermark and store it separately
  // for later reprocessing or analysis.
  def lateDataRecoveryWithSideOutput() = {
    val inputDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val eventTime = new Timestamp(tokens(0).toLong)
        val data = tokens(1)
        val processingTime = new Timestamp(System.currentTimeMillis())

        (eventTime, data, processingTime)
      }
      .toDF("eventTime", "data", "processingTime")

    // Main processing with watermark
    val mainStream = inputDF
      .withWatermark("eventTime", "5 seconds")
      .groupBy(window(col("eventTime"), "10 seconds"))
      .agg(
        count("*").as("recordCount"),
        collect_list("data").as("dataList")
      )
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("recordCount"),
        col("dataList"),
        lit("main_stream").as("streamType")
      )

    // Use foreachBatch to implement late data detection
    inputDF.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        import batch.sparkSession.implicits._
        
        if (!batch.isEmpty) {
          val currentWatermark = System.currentTimeMillis() - 5000 // 5 second watermark
          
          val (onTimeData, lateData) = batch.as[(java.sql.Timestamp, String, java.sql.Timestamp)]
            .collect()
            .partition(row => row._1.getTime >= currentWatermark)
          
          // Process on-time data
          val onTimeDF = spark.createDataset(onTimeData).toDF("eventTime", "data", "processingTime")
          val onTimeAgg = onTimeDF
            .groupBy(window(col("eventTime"), "10 seconds"))
            .agg(count("*").as("recordCount"), collect_list("data").as("dataList"))
          
          if (!onTimeAgg.isEmpty) {
            println(s"--- Batch $batchId: On-time data ---")
            onTimeAgg.show(false)
          }
          
          // Handle late data separately
          if (lateData.nonEmpty) {
            val lateDF = spark.createDataset(lateData).toDF("eventTime", "data", "processingTime")
            println(s"--- Batch $batchId: Late data (${lateData.length} records) ---")
            lateDF.select("eventTime", "data").show(false)
            
            // Could write late data to a separate sink for reprocessing
            // lateDF.write.mode("append").option("path", "/path/to/late-data").save()
          }
        }
      }
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()
  }

  // 4. Adaptive Watermark Adjustment
  // Dynamically adjust watermark based on observed data patterns and latency characteristics.
  def adaptiveWatermarkAdjustment() = {
    val inputDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val eventTime = new Timestamp(tokens(0).toLong)
        val data = tokens(1)
        val processingTime = new Timestamp(System.currentTimeMillis())
        val latency = processingTime.getTime - eventTime.getTime

        (eventTime, data, processingTime, latency)
      }
      .toDF("eventTime", "data", "processingTime", "latencyMs")

    inputDF.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batch.isEmpty) {
          import batch.sparkSession.implicits._
          
          // Analyze latency patterns
          val latencyStats = batch.agg(
            avg("latencyMs").as("avgLatency"),
            expr("percentile_approx(latencyMs, 0.95)").as("p95Latency"),
            max("latencyMs").as("maxLatency"),
            count("*").as("recordCount")
          ).collect().head
          
          val avgLatency = latencyStats.getAs[Double]("avgLatency")
          val p95Latency = latencyStats.getAs[Double]("p95Latency")
          val maxLatency = latencyStats.getAs[Long]("maxLatency")
          val recordCount = latencyStats.getAs[Long]("recordCount")
          
          // Adaptive watermark logic
          val recommendedWatermark = if (recordCount < 10) {
            "5 seconds" // Default for low volume
          } else if (p95Latency > 10000) { // > 10 seconds
            "30 seconds" // High latency, use longer watermark
          } else if (p95Latency > 5000) { // > 5 seconds
            "15 seconds" // Medium latency
          } else {
            "5 seconds" // Low latency, use shorter watermark
          }
          
          println(s"Batch $batchId Latency Analysis:")
          println(s"  Records: $recordCount")
          println(s"  Avg Latency: ${avgLatency.toLong}ms")
          println(s"  P95 Latency: ${p95Latency.toLong}ms")
          println(s"  Max Latency: ${maxLatency}ms")
          println(s"  Recommended Watermark: $recommendedWatermark")
          
          // Apply the recommended watermark (this would require restarting the query in practice)
          val processedData = batch
            .withWatermark("eventTime", recommendedWatermark)
            .groupBy(window(col("eventTime"), "10 seconds"))
            .agg(
              count("*").as("recordCount"),
              avg("latencyMs").as("avgLatency"),
              collect_list("data").as("dataList")
            )
          
          if (!processedData.isEmpty) {
            processedData.show(false)
          }
        }
      }
      .trigger(Trigger.ProcessingTime(5.seconds))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and use the DataSender to send data
    // multiLevelWatermarkStrategy()
    // watermarkImpactAnalysis()
    // lateDataRecoveryWithSideOutput()
    adaptiveWatermarkAdjustment()
  }
}

// sending data "manually" through socket to be as deterministic as possible
object DataSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept() // blocking call
  val printer = new PrintStream(socket.getOutputStream)

  println("socket accepted")

  def example1() = {
    Thread.sleep(7000)
    printer.println("7000,blue")
    Thread.sleep(1000)
    printer.println("8000,green")
    Thread.sleep(4000)
    printer.println("14000,blue")
    Thread.sleep(1000)
    printer.println("9000,red") // discarded: older than the watermark
    Thread.sleep(3000)
    printer.println("15000,red")
    printer.println("8000,blue") // discarded: older than the watermark
    Thread.sleep(1000)
    printer.println("13000,green")
    Thread.sleep(500)
    printer.println("21000,green")
    Thread.sleep(3000)
    printer.println("4000,purple") // expect to be dropped - it's older than the watermark
    Thread.sleep(2000)
    printer.println("17000,green")
  }

  def example2() = {
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("4000,blue")

    Thread.sleep(7000)
    printer.println("1000,yellow")
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")

    Thread.sleep(3000)
    printer.println("10000,pink")
  }

  def example3() = {
    Thread.sleep(2000)
    printer.println("9000,blue")
    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")
    Thread.sleep(2000)
    printer.println("5000,red") // discarded
    printer.println("18000,blue")
    Thread.sleep(1000)
    printer.println("2000,green") // discarded
    Thread.sleep(2000)
    printer.println("30000,purple")
    printer.println("10000,green")
  }

  def main(args: Array[String]): Unit = {
    example3()
  }
}

