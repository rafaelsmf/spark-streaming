package part6advanced

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object StatefulComputations {

  val spark = SparkSession.builder()
    .appName("Stateful Computation")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)
  case class SocialPostBulk(postType: String, count: Int, totalStorageUsed: Int)
  case class AveragePostStorage(postType: String, averageStorage: Double)


  // postType,count,storageUsed
  def readSocialUpdates() = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
      }

  def updateAverageStorage(
                          postType: String, // the key by which the grouping was made
                          group: Iterator[SocialPostRecord], // a batch of data associated to the key
                          state: GroupState[SocialPostBulk] // like an "option", I have to manage manually
                          ) : AveragePostStorage = { // a single value that I will output per the entire group

    /*
      - extract the state to start with
      - for all the items in the group
        - aggregate data:
          - summing up the total count
          - summing up the total storage
      - update the state with the new aggregated data
      - return a single value of type AveragePostStorage
     */

    // extract the state to start with
    val previousBulk =
      if (state.exists) state.get
      else SocialPostBulk(postType, 0, 0)

    // iterate through the group
    val totalAggregatedData: (Int, Int) = group.foldLeft((0, 0)) { (currentData, record) =>
      val (currentCount, currentStorage) = currentData
      (currentCount + record.count, currentStorage + record.storageUsed)
    }

    // update the state with new aggregated data
    val (totalCount, totalStorage) = totalAggregatedData
    val newPostBulk = SocialPostBulk(postType, previousBulk.count + totalCount, previousBulk.totalStorageUsed + totalStorage)
    state.update(newPostBulk)

    // return a single output value
    AveragePostStorage(postType, newPostBulk.totalStorageUsed * 1.0 / newPostBulk.count)
  }

  def getAveragePostStorage() = {
    val socialStream = readSocialUpdates()

    val regularSqlAverageByPostType = socialStream
      .groupByKey(_.postType)
      .agg(sum(col("count")).as("totalCount").as[Int], sum(col("storageUsed")).as("totalStorage").as[Int])
      .selectExpr("key as postType", "totalStorage/totalCount as avgStorage")

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateAverageStorage)

    averageByPostType.writeStream
      .outputMode("update") // append not supported on mapGroupsWithState
      .foreachBatch { (batch: Dataset[AveragePostStorage], _: Long ) =>
        batch.show()
      }
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. User Session Tracking with Timeout
  // Track user sessions where a session expires if no activity is seen for more than 30 seconds.
  // When a session expires, output the session summary (start time, end time, total events, duration).
  case class UserEvent(userId: String, eventType: String, timestamp: Long)
  case class UserSession(userId: String, startTime: Long, lastActivity: Long, eventCount: Int, eventTypes: Set[String])
  case class SessionSummary(userId: String, startTime: Long, endTime: Long, duration: Long, totalEvents: Int, uniqueEventTypes: Int)

  def trackUserSessionsWithTimeout() = {
    val userEventsStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        UserEvent(tokens(0), tokens(1), System.currentTimeMillis())
      }

    def updateSessionState(
      userId: String,
      events: Iterator[UserEvent],
      state: GroupState[UserSession]
    ): Iterator[SessionSummary] = {
      
      val currentTime = System.currentTimeMillis()
      val timeoutDuration = 30000L // 30 seconds
      
      // Check for timeout
      if (state.hasTimedOut) {
        val expiredSession = state.get
        state.remove()
        Iterator(SessionSummary(
          userId,
          expiredSession.startTime,
          expiredSession.lastActivity,
          expiredSession.lastActivity - expiredSession.startTime,
          expiredSession.eventCount,
          expiredSession.eventTypes.size
        ))
      } else {
        val eventsList = events.toList
        if (eventsList.nonEmpty) {
          val existingSession = state.getOption.getOrElse(
            UserSession(userId, currentTime, currentTime, 0, Set())
          )
          
          val newEventTypes = existingSession.eventTypes ++ eventsList.map(_.eventType).toSet
          val updatedSession = UserSession(
            userId,
            existingSession.startTime,
            currentTime,
            existingSession.eventCount + eventsList.length,
            newEventTypes
          )
          
          state.update(updatedSession)
          state.setTimeoutDuration(timeoutDuration)
          Iterator.empty // No output until session expires
        } else {
          Iterator.empty
        }
      }
    }

    val sessionSummaries = userEventsStream
      .groupByKey(_.userId)
      .flatMapGroupsWithState(
        outputMode = org.apache.spark.sql.streaming.OutputMode.Append(),
        timeoutConf = GroupStateTimeout.ProcessingTimeTimeout()
      )(updateSessionState)

    sessionSummaries.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }

  // 2. Real-time Fraud Detection with State
  // Detect potentially fraudulent activity by tracking user behavior patterns.
  // Flag users who have more than 5 transactions in the last minute or total amount > $10,000.
  case class Transaction(userId: String, amount: Double, timestamp: Long, transactionId: String)
  case class UserActivityState(userId: String, recentTransactions: List[Transaction], totalAmount: Double, transactionCount: Int)
  case class FraudAlert(userId: String, reason: String, transactionCount: Int, totalAmount: Double, suspiciousTransactions: List[String])

  def fraudDetectionWithState() = {
    val transactionStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        Transaction(tokens(0), tokens(1).toDouble, System.currentTimeMillis(), tokens(2))
      }

    def updateFraudState(
      userId: String,
      transactions: Iterator[Transaction],
      state: GroupState[UserActivityState]
    ): Iterator[FraudAlert] = {
      
      val currentTime = System.currentTimeMillis()
      val windowDuration = 60000L // 1 minute
      val maxTransactionsPerMinute = 5
      val maxAmountThreshold = 10000.0
      
      val newTransactions = transactions.toList
      val existingState = state.getOption.getOrElse(
        UserActivityState(userId, List(), 0.0, 0)
      )
      
      // Filter recent transactions (within 1 minute)
      val recentTransactions = (existingState.recentTransactions ++ newTransactions)
        .filter(t => currentTime - t.timestamp <= windowDuration)
      
      val totalAmount = recentTransactions.map(_.amount).sum
      val transactionCount = recentTransactions.length
      
      val updatedState = UserActivityState(
        userId,
        recentTransactions,
        totalAmount,
        transactionCount
      )
      state.update(updatedState)
      
      // Check for fraud conditions
      val alerts = scala.collection.mutable.ListBuffer[FraudAlert]()
      
      if (transactionCount > maxTransactionsPerMinute) {
        alerts += FraudAlert(
          userId,
          s"Too many transactions: $transactionCount in 1 minute",
          transactionCount,
          totalAmount,
          recentTransactions.map(_.transactionId)
        )
      }
      
      if (totalAmount > maxAmountThreshold) {
        alerts += FraudAlert(
          userId,
          s"High amount: $$${totalAmount} in 1 minute",
          transactionCount,
          totalAmount,
          recentTransactions.map(_.transactionId)
        )
      }
      
      // Check for unusual spending patterns (large amount compared to average)
      if (recentTransactions.nonEmpty) {
        val avgAmount = totalAmount / transactionCount
        val hasLargeTransaction = newTransactions.exists(_.amount > avgAmount * 5)
        if (hasLargeTransaction) {
          alerts += FraudAlert(
            userId,
            "Unusual large transaction detected",
            transactionCount,
            totalAmount,
            newTransactions.filter(_.amount > avgAmount * 5).map(_.transactionId)
          )
        }
      }
      
      alerts.iterator
    }

    val fraudAlerts = transactionStream
      .groupByKey(_.userId)
      .flatMapGroupsWithState(
        outputMode = org.apache.spark.sql.streaming.OutputMode.Append(),
        timeoutConf = GroupStateTimeout.NoTimeout()
      )(updateFraudState)

    fraudAlerts.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // 3. Dynamic Threshold Adjustment
  // Maintain adaptive thresholds that adjust based on historical patterns.
  // For example, adjust the threshold for "high" CPU usage based on recent observations.
  case class MetricReading(source: String, metricType: String, value: Double, timestamp: Long)
  case class ThresholdState(source: String, metricType: String, values: List[Double], currentThreshold: Double, adjustmentCount: Int)
  case class ThresholdAlert(source: String, metricType: String, currentValue: Double, threshold: Double, severity: String)

  def dynamicThresholdAdjustment() = {
    val metricsStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        MetricReading(tokens(0), tokens(1), tokens(2).toDouble, System.currentTimeMillis())
      }

    def updateThresholdState(
      key: (String, String), // (source, metricType)
      readings: Iterator[MetricReading],
      state: GroupState[ThresholdState]
    ): Iterator[ThresholdAlert] = {
      
      val (source, metricType) = key
      val newReadings = readings.toList
      
      if (newReadings.nonEmpty) {
        val existingState = state.getOption.getOrElse(
          ThresholdState(source, metricType, List(), 80.0, 0) // Initial threshold
        )
        
        // Keep only recent values (last 100 readings)
        val allValues = (existingState.values ++ newReadings.map(_.value)).takeRight(100)
        
        // Calculate new adaptive threshold based on historical data
        val newThreshold = if (allValues.length >= 10) {
          val mean = allValues.sum / allValues.length
          val variance = allValues.map(v => Math.pow(v - mean, 2)).sum / allValues.length
          val stdDev = Math.sqrt(variance)
          mean + (2 * stdDev) // 2 standard deviations above mean
        } else {
          existingState.currentThreshold
        }
        
        val updatedState = ThresholdState(
          source,
          metricType,
          allValues,
          newThreshold,
          existingState.adjustmentCount + (if (Math.abs(newThreshold - existingState.currentThreshold) > 5) 1 else 0)
        )
        state.update(updatedState)
        
        // Generate alerts for readings that exceed the threshold
        val alerts = newReadings.flatMap { reading =>
          if (reading.value > updatedState.currentThreshold) {
            val severity = if (reading.value > updatedState.currentThreshold * 1.5) "CRITICAL" 
                          else if (reading.value > updatedState.currentThreshold * 1.2) "HIGH" 
                          else "MEDIUM"
            
            Some(ThresholdAlert(source, metricType, reading.value, updatedState.currentThreshold, severity))
          } else {
            None
          }
        }
        
        alerts.iterator
      } else {
        Iterator.empty
      }
    }

    val thresholdAlerts = metricsStream
      .groupByKey(reading => (reading.source, reading.metricType))
      .flatMapGroupsWithState(
        outputMode = org.apache.spark.sql.streaming.OutputMode.Append(),
        timeoutConf = GroupStateTimeout.NoTimeout()
      )(updateThresholdState)

    thresholdAlerts.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and start a socket session: nc -lk 12345
    // For trackUserSessionsWithTimeout: send "user1,login" "user1,click" etc.
    // For fraudDetectionWithState: send "user1,500.0,txn001" "user1,1000.0,txn002" etc.
    // For dynamicThresholdAdjustment: send "server1,cpu,85.5" "server1,cpu,92.1" etc.
    dynamicThresholdAdjustment()
  }
}

/*
-- batch 1
text,3,3000
text,4,5000
video,1,500000
audio,3,60000
-- batch 2
text,1,2500

average for text = 10500 / 8
 */