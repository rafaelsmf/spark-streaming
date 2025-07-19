package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJoins {

  val spark = SparkSession.builder()
    .appName("Streaming Joins")
    .master("local[2]")
    .getOrCreate()

  val guitarPlayers = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers")

  val guitars = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars")

  val bands = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands")

  // joining static DFs
  val joinCondition = guitarPlayers.col("band") === bands.col("id")
  val guitaristsBands = guitarPlayers.join(bands, joinCondition, "inner")
  val bandsSchema = bands.schema

  def joinStreamWithStatic() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    // join happens PER BATCH
    val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers, guitarPlayers.col("band") === streamedBandsDF.col("id"), "inner")

    /*
      restricted joins:
      - stream joining with static: RIGHT outer join/full outer join/right_semi not permitted
      - static joining with streaming: LEFT outer join/full/left_semi not permitted
     */

    streamedBandsGuitaristsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // since Spark 2.3 we have stream vs stream joins
  def joinStreamWithStream() = {
    val streamedBandsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load() // a DF with a single column "value" of type String
      .select(from_json(col("value"), bandsSchema).as("band"))
      .selectExpr("band.id as id", "band.name as name", "band.hometown as hometown", "band.year as year")

    val streamedGuitaristsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .select(from_json(col("value"), guitarPlayers.schema).as("guitarPlayer"))
      .selectExpr("guitarPlayer.id as id", "guitarPlayer.name as name", "guitarPlayer.guitars as guitars", "guitarPlayer.band as band")

    // join stream with stream
    val streamedJoin = streamedBandsDF.join(streamedGuitaristsDF, streamedGuitaristsDF.col("band") === streamedBandsDF.col("id"))

    /*
      - inner joins are supported
      - left/right outer joins ARE supported, but MUST have watermarks
      - full outer joins are NOT supported
     */

    streamedJoin.writeStream
      .format("console")
      .outputMode("append") // only append supported for stream vs stream join
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Stream-Stream Join with Watermarks and Windowing
  //    Join two streams of events: ad impressions and ad clicks.
  //    An impression is ("adId", "impressionTime"). A click is ("adId", "clickTime").
  //    Join them on adId, but only if the click happens within 10 minutes of the impression.
  //    Use watermarks to handle late data.
  def adAnalyticsJoin(): Unit = {
    import spark.implicits._

    val impressionsStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val parts = line.split(",")
        (parts(0), new java.sql.Timestamp(parts(1).toLong))
      }
      .toDF("impressionAdId", "impressionTime")

    val clicksStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .as[String]
      .map { line =>
        val parts = line.split(",")
        (parts(0), new java.sql.Timestamp(parts(1).toLong))
      }
      .toDF("clickAdId", "clickTime")

    // Apply watermarks
    val impressionsWithWatermark = impressionsStream.withWatermark("impressionTime", "10 minutes")
    val clicksWithWatermark = clicksStream.withWatermark("clickTime", "10 minutes")

    // Join condition
    val joinExpr = col("impressionAdId") === col("clickAdId") &&
      col("clickTime").between(
        col("impressionTime"),
        col("impressionTime") + expr("interval 10 minutes")
      )

    val joinedDF = impressionsWithWatermark.join(clicksWithWatermark, joinExpr, "inner")

    joinedDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // 2. Left Outer Join with State Cleanup
  //    Enrich a stream of user actions with user profile data.
  //    Actions: ("userId", "action"). Profiles: ("userId", "name", "registrationDate").
  //    Profiles can be updated. Use a left outer join.
  //    Remove user state if no actions are seen for that user for 1 hour.
  case class UserAction(userId: String, action: String, actionTime: java.sql.Timestamp)
  case class UserProfile(userId: String, name: String, registrationDate: java.sql.Timestamp)
  case class EnrichedAction(userId: String, action: String, actionTime: java.sql.Timestamp, name: Option[String], registrationDate: Option[java.sql.Timestamp])
  case class UserState(profile: UserProfile, lastActionTime: java.sql.Timestamp)

  def userEnrichmentWithStateCleanup(): Unit = {
    import spark.implicits._

    val actionStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val parts = line.split(",")
        UserAction(parts(0), parts(1), new java.sql.Timestamp(System.currentTimeMillis()))
      }

    val profileStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .as[String]
      .map { line =>
        val parts = line.split(",")
        UserProfile(parts(0), parts(1), new java.sql.Timestamp(parts(2).toLong))
      }

    // We can't directly do a stream-stream left outer join and then manage state with flatMapGroupsWithState.
    // Instead, we can use flatMapGroupsWithState on a unioned/co-grouped stream, or use a more complex foreachBatch logic.
    // A more direct approach is to use a left outer join with watermarks.

    val actionsWithWatermark = actionStream.withWatermark("actionTime", "1 hour")
    val profilesWithWatermark = profileStream.withWatermark("registrationDate", "1 hour") // Watermark on profiles is tricky, depends on update logic. Let's assume updates also have a timestamp.

    val enrichedStream = actionsWithWatermark.join(
      profilesWithWatermark,
      expr("userId"), // join key
      "leftOuter"
    )

    enrichedStream.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
    // Note: True state cleanup for inactive users in a join scenario is complex.
    // flatMapGroupsWithState is a better tool for this, by creating a composite stream of actions and profile updates.
  }

  // 3. Interval Joins for IoT Sensor Data
  //    Join two streams of sensor data: "start" events and "end" events.
  //    Start: ("deviceId", "startTime"). End: ("deviceId", "endTime").
  //    Join them if the end event happens between 10 and 30 seconds after the start event.
  def iotIntervalJoin(): Unit = {
    import spark.implicits._

    val startStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map(line => (line, new java.sql.Timestamp(System.currentTimeMillis())))
      .toDF("startDeviceId", "startTime")

    val endStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12346)
      .load()
      .as[String]
      .map(line => (line, new java.sql.Timestamp(System.currentTimeMillis())))
      .toDF("endDeviceId", "endTime")

    val startWithWatermark = startStream.withWatermark("startTime", "1 minute")
    val endWithWatermark = endStream.withWatermark("endTime", "1 minute")

    val joinExpr =
      col("startDeviceId") === col("endDeviceId") &&
      col("endTime").between(
        col("startTime") + expr("interval 10 seconds"),
        col("startTime") + expr("interval 30 seconds")
      )

    val joinedDF = startWithWatermark.join(endWithWatermark, joinExpr, "inner")

    joinedDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and start netcat sessions.
    // For adAnalyticsJoin:
    // nc -lk 12345 -> ad1,1640995200000 (impression)
    // nc -lk 12346 -> ad1,1640995210000 (click)
    // adAnalyticsJoin()

    // For userEnrichmentWithStateCleanup:
    // nc -lk 12345 -> user1,click
    // nc -lk 12346 -> user1,John Doe,1640995200000
    // userEnrichmentWithStateCleanup()

    // For iotIntervalJoin:
    // nc -lk 12345 -> deviceA
    // nc -lk 12346 -> deviceA (within 10-30s)
    iotIntervalJoin()
  }
}
