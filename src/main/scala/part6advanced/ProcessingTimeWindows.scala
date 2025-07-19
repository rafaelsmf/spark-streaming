package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ProcessingTimeWindows {

  val spark = SparkSession.builder()
    .appName("Processing Time Windows")
    .master("local[2]")
    .getOrCreate()

  def aggregateByProcessingTime() = {
    val linesCharCountByWindowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(col("value"), current_timestamp().as("processingTime")) // this is how you add processing time to a record
      .groupBy(window(col("processingTime"),  "10 seconds").as("window"))
      .agg(sum(length(col("value"))).as("charCount")) // counting characters every 10 seconds by processing time
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("charCount")
      )

    linesCharCountByWindowDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Processing Time Lag Analysis
  // Calculate the difference between event time and processing time to measure processing lag.
  // This helps identify performance bottlenecks and late-arriving data patterns.
  def processingTimeLagAnalysis() = {
    val lagAnalysisDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(
        col("value"),
        current_timestamp().as("processingTime")
      )
      .filter(col("value").contains("timestamp=")) // Expect input like "data,timestamp=2023-01-01T10:00:00Z"
      .withColumn("eventTime", 
        to_timestamp(regexp_extract(col("value"), "timestamp=([^,]+)", 1), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn("lagSeconds", 
        (unix_timestamp(col("processingTime")) - unix_timestamp(col("eventTime"))))
      .groupBy(window(col("processingTime"), "30 seconds").as("window"))
      .agg(
        count("*").as("recordCount"),
        avg("lagSeconds").as("avgLagSeconds"),
        min("lagSeconds").as("minLagSeconds"),
        max("lagSeconds").as("maxLagSeconds"),
        expr("percentile_approx(lagSeconds, 0.95)").as("p95LagSeconds")
      )
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("recordCount"),
        col("avgLagSeconds"),
        col("minLagSeconds"),
        col("maxLagSeconds"),
        col("p95LagSeconds")
      )

    lagAnalysisDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // 2. Throughput Monitoring with Processing Time Windows
  // Monitor the throughput of your streaming application by tracking records processed per second
  // across different processing time windows. Include memory usage estimation.
  def throughputMonitoring() = {
    val throughputDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(
        col("value"),
        current_timestamp().as("processingTime"),
        lit(1).as("recordCount"),
        length(col("value")).as("dataSize")
      )
      .groupBy(window(col("processingTime"), "10 seconds").as("window"))
      .agg(
        sum("recordCount").as("totalRecords"),
        sum("dataSize").as("totalDataSize"),
        avg("dataSize").as("avgRecordSize")
      )
      .withColumn("recordsPerSecond", col("totalRecords") / 10.0)
      .withColumn("bytesPerSecond", col("totalDataSize") / 10.0)
      .withColumn("estimatedMemoryMB", col("totalDataSize") / (1024 * 1024))
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("totalRecords"),
        col("recordsPerSecond"),
        col("bytesPerSecond"),
        col("avgRecordSize"),
        col("estimatedMemoryMB")
      )

    throughputDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // 3. Real-time Anomaly Detection using Processing Time Windows
  // Detect anomalies in data patterns by comparing current window statistics 
  // with historical baselines using z-score analysis.
  def realTimeAnomalyDetection() = {
    import spark.implicits._
    
    val dataWithStats = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(
        col("value"),
        current_timestamp().as("processingTime")
      )
      .filter(col("value").rlike("^\\d+$")) // Expect numeric values
      .withColumn("numericValue", col("value").cast("double"))
      .groupBy(window(col("processingTime"), "20 seconds").as("window"))
      .agg(
        count("numericValue").as("recordCount"),
        avg("numericValue").as("avgValue"),
        stddev("numericValue").as("stddevValue"),
        min("numericValue").as("minValue"),
        max("numericValue").as("maxValue")
      )
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("recordCount"),
        col("avgValue"),
        col("stddevValue"),
        col("minValue"),
        col("maxValue")
      )

    // Use foreachBatch to implement anomaly detection logic
    dataWithStats.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batch.isEmpty) {
          val currentStats = batch.collect()
          
          // Simple anomaly detection: compare with expected ranges
          currentStats.foreach { row =>
            val avgValue = row.getAs[Double]("avgValue")
            val stddevValue = Option(row.getAs[Double]("stddevValue")).getOrElse(0.0)
            val recordCount = row.getAs[Long]("recordCount")
            
            // Expected ranges (these would typically come from historical data)
            val expectedAvg = 50.0
            val expectedStddev = 10.0
            val expectedCount = 100L
            
            // Calculate z-scores
            val avgZScore = if (expectedStddev > 0) Math.abs(avgValue - expectedAvg) / expectedStddev else 0.0
            val countDeviation = Math.abs(recordCount.toDouble - expectedCount.toDouble) / expectedCount.toDouble
            
            val isAnomaly = avgZScore > 2.0 || countDeviation > 0.5 || stddevValue > expectedStddev * 2
            
            if (isAnomaly) {
              println(s"ANOMALY DETECTED in batch $batchId:")
              println(s"  Window: ${row.getAs[java.sql.Timestamp]("windowStart")} to ${row.getAs[java.sql.Timestamp]("windowEnd")}")
              println(s"  Avg Value: $avgValue (z-score: $avgZScore)")
              println(s"  Record Count: $recordCount (deviation: ${countDeviation * 100}%)")
              println(s"  Std Dev: $stddevValue")
            }
          }
          
          batch.show(false)
        }
      }
      .start()
      .awaitTermination()
  }

  // 4. Adaptive Batch Processing based on Processing Time
  // Dynamically adjust the trigger interval based on system load and processing time patterns.
  // When processing is fast, increase frequency. When processing is slow, decrease frequency.
  def adaptiveBatchProcessing() = {
    import org.apache.spark.sql.streaming.Trigger
    import scala.concurrent.duration._
    
    val dataStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(
        col("value"),
        current_timestamp().as("processingTime"),
        lit(System.currentTimeMillis()).as("systemTime")
      )

    // Track processing efficiency in a simple aggregation
    val efficiencyMetrics = dataStream
      .groupBy(window(col("processingTime"), "30 seconds").as("window"))
      .agg(
        count("*").as("recordCount"),
        min("systemTime").as("batchStartTime"),
        max("systemTime").as("batchEndTime")
      )
      .withColumn("processingDurationMs", col("batchEndTime") - col("batchStartTime"))
      .withColumn("recordsPerMs", col("recordCount") / (col("processingDurationMs") + 1))
      .select(
        col("window").getField("start").as("windowStart"),
        col("window").getField("end").as("windowEnd"),
        col("recordCount"),
        col("processingDurationMs"),
        col("recordsPerMs")
      )

    // Use foreachBatch to implement adaptive logic
    efficiencyMetrics.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batch.isEmpty) {
          val metrics = batch.collect()
          
          metrics.foreach { row =>
            val recordsPerMs = row.getAs[Double]("recordsPerMs")
            val processingDuration = row.getAs[Long]("processingDurationMs")
            
            // Adaptive logic
            val recommendedInterval = recordsPerMs match {
              case rate if rate > 1.0 => "5 seconds"   // High efficiency, process more frequently
              case rate if rate > 0.5 => "10 seconds"  // Medium efficiency, standard interval
              case rate if rate > 0.1 => "20 seconds"  // Low efficiency, reduce frequency
              case _ => "30 seconds"                    // Very low efficiency, minimal frequency
            }
            
            println(s"Batch $batchId Performance Analysis:")
            println(s"  Records/ms: $recordsPerMs")
            println(s"  Processing Duration: ${processingDuration}ms")
            println(s"  Recommended Interval: $recommendedInterval")
            println(s"  Current Window: ${row.getAs[java.sql.Timestamp]("windowStart")} to ${row.getAs[java.sql.Timestamp]("windowEnd")}")
          }
          
          batch.show(false)
        }
      }
      .trigger(Trigger.ProcessingTime(10.seconds)) // Start with 10-second intervals
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and start a socket session: nc -lk 12346
    // For processingTimeLagAnalysis: send "data1,timestamp=2023-01-01T10:00:00Z"
    // For throughputMonitoring: send any text data
    // For realTimeAnomalyDetection: send numeric values like "42", "55", "38"
    // For adaptiveBatchProcessing: send any data and observe the adaptation recommendations
    adaptiveBatchProcessing()
  }
}
