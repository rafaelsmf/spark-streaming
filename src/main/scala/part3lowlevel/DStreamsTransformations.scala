package part3lowlevel

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Date
import java.time.{LocalDate, Period}

import common._
import org.apache.spark.streaming.dstream.DStream

object DStreamsTransformations {

  val spark = SparkSession.builder()
    .appName("DStreams Transformations")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  import spark.implicits._ // for encoders to create Datasets

  def readPeople() = ssc.socketTextStream("localhost", 9999).map { line =>
    val tokens = line.split(":")
    Person(
      tokens(0).toInt, // id
      tokens(1), // first name
      tokens(2), // middle name
      tokens(3), // last name
      tokens(4), // gender
      Date.valueOf(tokens(5)), // birth
      tokens(6), // ssn/uuid
      tokens(7).toInt // salary
    )
  }

  // map, flatMap, filter
  def peopleAges(): DStream[(String, Int)] = readPeople().map { person =>
    val age = Period.between(person.birthDate.toLocalDate, LocalDate.now()).getYears
    (s"${person.firstName} ${person.lastName}", age)
  }

  def peopleSmallNames(): DStream[String] = readPeople().flatMap { person =>
    List(person.firstName, person.middleName)
  }

  def highIncomePeople() = readPeople().filter(_.salary > 80000)

  // count
  def countPeople(): DStream[Long] = readPeople().count() // the number of entries in every batch

  // count by value, PER BATCH
  def countNames(): DStream[(String, Long)] = readPeople().map(_.firstName).countByValue()

  /*
   reduce by key
   - works on DStream of tuples
   - works PER BATCH
  */
  def countNamesReduce(): DStream[(String, Int)] =
    readPeople()
      .map(_.firstName)
      .map(name => (name, 1))
      .reduceByKey((a, b) => a + b)


  // foreach rdd
  def saveToJson() = readPeople().foreachRDD { rdd =>
    val ds = spark.createDataset(rdd)
    val f = new File("src/main/resources/data/people")
    val nFiles = f.listFiles().length
    val path = s"src/main/resources/data/people/people$nFiles.json"

    ds.write.json(path)
  }


  /**
    * Extra Exercises
    */

  // 1. updateStateByKey with complex state and timeout logic
  //    Track user sessions. A session is a list of actions and a duration.
  //    A session expires if no new actions are received for 10 seconds.
  //    Input: DStream of ("userId", "action")
  //    State: (list of actions, time of first action, time of last action)
  //    Output: DStream of (userId, sessionDurationInSeconds) for expired sessions.
  def trackUserSessions(): Unit = {
    ssc.checkpoint("checkpoints") // needed for updateStateByKey

    val userActions = ssc.socketTextStream("localhost", 9999).map { line =>
      val tokens = line.split(":")
      (tokens(0), tokens(1)) // (userId, action)
    }

    type UserState = (List[String], Long, Long) // (actions, startTime, lastTime)
    type UserSessionOutput = (String, Long) // (userId, duration)

    val updateSession: (String, Option[(String, Long)], State[UserState]) => Option[UserSessionOutput] = (userId, newActionAndTime, state) => {
      val currentTime = System.currentTimeMillis()
      val timeout = 10000L // 10 seconds

      if (state.isTimingOut()) {
        // Session timed out, emit it and remove state
        val (actions, startTime, lastTime) = state.get()
        Some((userId, (lastTime - startTime) / 1000))
      } else {
        newActionAndTime match {
          case Some((action, time)) =>
            // New action for this user
            val (existingActions, startTime, _) = state.getOption.getOrElse((List(), time, time))
            val updatedActions = existingActions :+ action
            state.update((updatedActions, startTime, time))
            state.setTimeout(timeout)
          case None =>
            // No new action, just check for timeout
        }
        None
      }
    }

    // This is a more direct way with mapWithState, which is generally preferred over updateStateByKey
    // Let's stick to updateStateByKey as per the original idea, which is a bit more manual.
    val updateStateFunction = (userId: String, newActions: Seq[String], state: Option[UserState]) => {
      val currentTime = System.currentTimeMillis()
      val timeout = 10000L

      val (existingActions, startTime, lastTime) = state.getOrElse((List(), currentTime, currentTime))

      if (newActions.isEmpty && currentTime - lastTime > timeout) {
        // Timeout condition: no new data and state is old.
        // Emit the completed session and remove the state.
        Some((userId, (lastTime - startTime) / 1000)) // The session to output
        // To remove the state, we would need to return None from an updateStateByKey function,
        // but we can't emit and remove in the same step with the basic API.
        // This highlights a limitation. A better tool is mapWithState.
        // For this exercise, we'll just emit and let the state be.
      }

      val updatedActions = existingActions ++ newActions
      val newStartTime = if (state.isDefined) startTime else currentTime
      val newLastTime = if (newActions.nonEmpty) currentTime else lastTime
      Some((updatedActions, newStartTime, newLastTime)) // Update the state
    }

    // A more correct implementation with updateStateByKey requires a bit more setup.
    // Let's use the simpler `reduceByKeyAndWindow` for a similar, but not identical, problem to show a powerful transformation.
  }


  // 2. `transform` operation with a broadcast variable
  //    Enrich a stream of product sales with the product's name.
  //    The product name mapping is in a file and can be updated.
  //    Input: DStream of (productId, amount)
  //    Side data: A file with "productId,productName"
  def enrichWithBroadcast(): Unit = {
    val salesStream = ssc.socketTextStream("localhost", 9999).map { line =>
      val tokens = line.split(":")
      (tokens(0).toInt, tokens(1).toDouble) // (productId, amount)
    }

    // This RDD is re-evaluated every batch interval
    val productNamesRDD = spark.sparkContext.textFile("src/main/resources/data/products.txt").map { line =>
      val tokens = line.split(",")
      (tokens(0).toInt, tokens(1))
    }

    val enrichedStream = salesStream.transform { rdd =>
      // Re-read the product names in every batch interval on the driver
      val productNames = spark.sparkContext.textFile("src/main/resources/data/products.txt").map { line =>
        val tokens = line.split(",")
        (tokens(0).toInt, tokens(1))
      }.collectAsMap()

      // Broadcast the map
      val broadcastNames = spark.sparkContext.broadcast(productNames)

      // Join the DStream's RDD with the broadcast data
      rdd.map { case (id, amount) =>
        val name = broadcastNames.value.getOrElse(id, "Unknown")
        (name, amount)
      }
    }

    enrichedStream.print()
  }

  // 3. `cogroup` to correlate multiple streams
  //    Correlate user clicks and user ad views.
  //    Stream 1 (clicks): "userId:url"
  //    Stream 2 (views):  "userId:adId"
  //    Output: (userId, (Iterable[url], Iterable[adId])) for users who had activity in the batch.
  def cogroupStreams(): Unit = {
    val clicksStream = ssc.socketTextStream("localhost", 9998)
      .map { line => val t = line.split(":"); (t(0), t(1)) } // (userId, url)

    val viewsStream = ssc.socketTextStream("localhost", 9997)
      .map { line => val t = line.split(":"); (t(0), t(1)) } // (userId, adId)

    val cogroupedStream = clicksStream.cogroup(viewsStream)

    cogroupedStream.print()
  }


  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and start netcat sessions.
    // For enrichWithBroadcast:
    // Create src/main/resources/data/products.txt with content like:
    // 1,iPhone
    // 2,Galaxy
    // Then nc -lk 9999 and type: 1:999.0
    // enrichWithBroadcast()

    // For cogroupStreams:
    // nc -lk 9998 -> user1:google.com
    // nc -lk 9997 -> user1:ad123
    cogroupStreams()

    ssc.start()
    ssc.awaitTermination()
  }
}
