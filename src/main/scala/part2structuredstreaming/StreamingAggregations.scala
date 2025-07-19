package part2structuredstreaming

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {

  val spark = SparkSession.builder()
    .appName("Streaming Aggregations")
    .master("local[2]")
    .getOrCreate()

  def streamingCount() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val lineCount: DataFrame = lines.selectExpr("count(*) as lineCount")

    // aggregations with distinct are not supported
    // otherwise Spark will need to keep track of EVERYTHING

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations(aggFunction: Column => Column): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // counting occurrences of the "name" value
    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name")) // RelationalGroupedDataset
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Implement a windowed aggregation that counts events per 10-second window
   * Expected input format: timestamps as longs (milliseconds since epoch)
   * Output should show the window start/end times and count of events in each window
   */
  def windowedAggregation(): Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // Parse incoming timestamps and add event_time column
    val timestampDF = lines
      .select(col("value").cast("long").as("timestamp"))
      .withColumn("event_time", to_timestamp(col("timestamp") / 1000))

    // Group by window of 10 seconds
    val windowedCounts = timestampDF
      .groupBy(window(col("event_time"), "10 seconds"))
      .count()
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("count")
      )

    windowedCounts.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Implement a stateful streaming aggregation using mapGroupsWithState
   * Each input line should contain: userId,action
   * Maintain a state that tracks the sequence of actions per user and the time of their last action
   */
  def statefulUserActivity(): Unit = {
    import spark.implicits._

    // Define case classes for our state handling
    case class UserAction(userId: String, action: String, timestamp: Long)
    case class UserState(userId: String, actions: List[String], lastSeen: Long)
    case class UserSummary(userId: String, actionCount: Int, lastSeen: Long)

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // Parse input lines into UserAction objects
    val userActions = lines
      .as[String]
      .map { line =>
        val parts = line.split(",")
        UserAction(parts(0), parts(1), System.currentTimeMillis())
      }

    // Define the state update function
    import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

    def updateUserState(
                         userId: String,
                         actions: Iterator[UserAction],
                         state: GroupState[UserState]
                       ): UserSummary = {

      // Get previous or initialize new state
      val currentState = if (state.exists) state.get else UserState(userId, List(), 0L)

      // Update state with new actions
      val newActions = actions.toList
      val updatedState = if (newActions.isEmpty) {
        currentState
      } else {
        val latestTimestamp = newActions.map(_.timestamp).max
        val updatedActions = currentState.actions ++ newActions.map(_.action)
        UserState(userId, updatedActions, latestTimestamp)
      }

      // Update the state and return summary
      state.update(updatedState)
      UserSummary(userId, updatedState.actions.size, updatedState.lastSeen)
    }

    // Apply the stateful operation
    val userActivitySummary = userActions
      .groupByKey(_.userId)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateUserState)

    userActivitySummary.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .start()
      .awaitTermination()
  }

  /**
   * Implement a streaming join between two socket sources
   * First stream: userId,userName (user profile updates)
   * Second stream: userId,itemId,action (user activity)
   * Join these streams to produce enriched activity events with user names
   */
  def streamingJoin(): Unit = {
    import spark.implicits._

    // Stream 1: User profiles
    val userProfiles = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val parts = line.split(",")
        (parts(0), parts(1)) // (userId, userName)
      }
      .toDF("userId", "userName")

    // Stream 2: User activities on port 12346
    val userActivities = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .as[String]
      .map { line =>
        val parts = line.split(",")
        (parts(0), parts(1), parts(2)) // (userId, itemId, action)
      }
      .toDF("userId", "itemId", "action")

    // Add watermarks to both streams (required for stream-to-stream joins)
    val profilesWithWatermark = userProfiles
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "1 minute")

    val activitiesWithWatermark = userActivities
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "1 minute")

    // Join the streams
    val joinedDF = activitiesWithWatermark.join(
      profilesWithWatermark,
      "userId"
    )

    // Output the enriched events
    joinedDF
      .select("userId", "userName", "itemId", "action", "timestamp")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 4. Implement a sliding window aggregation to calculate the moving average of a value.
  //    Input from socket: "key,value" (e.g., "sensorA,25.5")
  //    Calculate the moving average for each key over a 1-minute window that slides every 10 seconds.
  def movingAverage(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val dataDF = lines.select(
      split(col("value"), ",").getItem(0).as("key"),
      split(col("value"), ",").getItem(1).cast("double").as("value"),
      current_timestamp().as("timestamp")
    )

    val movingAvgDF = dataDF
      .withWatermark("timestamp", "2 minutes")
      .groupBy(
        col("key"),
        window(col("timestamp"), "1 minute", "10 seconds")
      )
      .agg(
        avg("value").as("moving_average"),
        count("value").as("event_count")
      )
      .select("key", "window.start", "window.end", "moving_average", "event_count")

    movingAvgDF.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // 5. Detect anomalies in a stream of events.
  //    Anomaly is defined as a key whose count in a 30-second window is more than twice the average count of all keys in that same window.
  def anomalyDetection(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(col("value").as("key"), current_timestamp().as("timestamp"))

    val windowedCounts = lines
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window(col("timestamp"), "30 seconds").as("window"),
        col("key")
      )
      .count()

    windowedCounts.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.persist()
          val windowAvg = batchDF
            .groupBy("window")
            .agg(avg("count").as("avg_count"))

          val anomalies = batchDF.join(windowAvg, "window")
            .filter(col("count") > col("avg_count") * 2)
            .select("window", "key", "count", "avg_count")

          if (!anomalies.isEmpty) {
            println(s"--- Batch $batchId: Anomalies Detected ---")
            anomalies.show(false)
          }
          batchDF.unpersist()
        }
      }
      .start()
      .awaitTermination()
  }

  // 6. Implement a custom stateful aggregation using flatMapGroupsWithState to manage user sessions.
  //    A session expires after 10 seconds of inactivity. Emit sessions as they expire.
  //    Input: "userId,action"
  case class UserAction(userId: String, action: String, timestamp: org.apache.spark.sql.Timestamp)
  case class SessionInfo(userId: String, actions: List[String], startTime: org.apache.spark.sql.Timestamp, lastUpdated: org.apache.spark.sql.Timestamp)
  case class SessionOutput(userId: String, actions: List[String], durationSeconds: Long)

  def sessionizationWithTimeout(): Unit = {
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val actionsDS = lines.as[String].map { line =>
      val parts = line.split(",")
      UserAction(parts(0), parts(1), new org.apache.spark.sql.Timestamp(System.currentTimeMillis()))
    }

    val sessionsDS = actionsDS
      .withWatermark("timestamp", "1 minute")
      .groupByKey(_.userId)
      .flatMapGroupsWithState(OutputMode.Append(), org.apache.spark.sql.streaming.GroupStateTimeout.ProcessingTimeTimeout) {
        case (userId: String, actions: Iterator[UserAction], state: GroupState[SessionInfo]) =>
          if (state.hasTimedOut) {
            val finalState = state.get
            state.remove()
            Iterator(SessionOutput(userId, finalState.actions, (finalState.lastUpdated.getTime - finalState.startTime.getTime) / 1000))
          } else {
            val newActions = actions.toList
            val currentState = state.getOption.getOrElse(SessionInfo(userId, List(), newActions.head.timestamp, newActions.head.timestamp))
            val updatedActions = currentState.actions ++ newActions.map(_.action)
            val lastTimestamp = newActions.map(_.timestamp).max
            state.update(SessionInfo(userId, updatedActions, currentState.startTime, lastTimestamp))
            state.setTimeoutDuration("10 seconds")
            Iterator.empty
          }
      }

    sessionsDS.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and start a netcat session: nc -lk 12345
    // For movingAverage: sensorA,25.5
    // For anomalyDetection: keyA
    // For sessionizationWithTimeout: user1,click
    sessionizationWithTimeout()
  }
}
