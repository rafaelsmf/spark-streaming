package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import org.apache.spark.sql.types._

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("Our first streams")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket() = {
    // reading a DF
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // transformation
    val shortLines: DataFrame = lines.filter(length(col("value")) <= 5)

    // tell between a static vs a streaming DF
    println(shortLines.isStreaming)

    // consuming a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        // Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query
        // Trigger.Once() // single batch, then terminate
        Trigger.Continuous(2.seconds) // experimental, every 2 seconds create a batch with whatever you have
      )
      .start()
      .awaitTermination()
  }

  /**
    * Exercises
    */

  // 1. Read a stream of JSON strings from a socket, with a complex schema.
  //    - Separate valid JSONs from invalid ones.
  //    - Write the invalid ones to the console.
  //    - For the valid ones, select all fields and write them to the console.
  def processComplexJsonStream(): Unit = {
    val complexJsonSchema = StructType(Array(
      StructField("id", StringType),
      StructField("user", StructType(Array(
        StructField("name", StringType),
        StructField("email", StringType)
      ))),
      StructField("timestamp", TimestampType),
      StructField("payload", StringType)
    ))

    val jsonStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(col("value").cast("string"))

    val parsedJsonStream = jsonStream.select(
      from_json(col("value"), complexJsonSchema, Map("mode" -> "PERMISSIVE")).as("parsed_json"),
      col("value").as("original_json")
    )

    val validJsonDF = parsedJsonStream.filter(col("parsed_json").isNotNull)
    val invalidJsonDF = parsedJsonStream.filter(col("parsed_json").isNull).select("original_json")

    val validQuery = validJsonDF
      .select("parsed_json.*")
      .writeStream
      .format("console")
      .outputMode("append")
      .queryName("ValidJSONs")
      .start()

    val invalidQuery = invalidJsonDF
      .writeStream
      .format("console")
      .outputMode("append")
      .queryName("InvalidJSONs")
      .start()

    validQuery.awaitTermination()
    invalidQuery.awaitTermination()
  }

  // 2. Implement a custom sink using foreachBatch.
  //    - The sink should write to a simulated key-value store (a mutable Map).
  //    - It should perform an "upsert" operation: if the key exists, update the value; otherwise, insert it.
  //    - The key should be a column from the DataFrame, and the value another column.
  def customKeyValueSink(): Unit = {
    import scala.collection.mutable
    val keyValueStore = mutable.Map[String, String]()

    val dataStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load()
      .select(
        (col("value") % 100).cast("string").as("key"),
        col("timestamp").cast("string").as("value")
      )

    dataStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"Processing batch $batchId")
        batchDF.persist() // Persist to avoid recomputation
        batchDF.foreachPartition { partition =>
          // This code runs on the executor
          partition.foreach { row =>
            val key = row.getAs[String]("key")
            val value = row.getAs[String]("value")
            // In a real scenario, you'd have a connection to a real KV store here.
            // For this exercise, we simulate it by updating a shared map.
            // Note: This is not thread-safe and only for demonstration on a single driver.
            keyValueStore.synchronized {
              keyValueStore(key) = value
            }
          }
        }
        println(s"Current KV store size: ${keyValueStore.size}")
        batchDF.unpersist()
      }
      .start()
      .awaitTermination()
  }

  // 3. Process a stream with dynamic configuration updates.
  //    - The stream processes numbers and filters them based on a threshold.
  //    - This threshold is read from a configuration file at the beginning of each batch.
  def processWithDynamicConfig(): Unit = {
    import scala.io.Source

    val configFilePath = "src/main/resources/data/config/threshold.txt"

    // Helper function to read the threshold
    def readThreshold(): Int = {
      try {
        Source.fromFile(configFilePath).getLines().next().toInt
      } catch {
        case _: Exception => 0 // Default threshold if file not found or invalid
      }
    }

    val numberStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()
      .select(col("value"))

    numberStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val currentThreshold = readThreshold()
        println(s"Processing batch $batchId with threshold: $currentThreshold")

        val filteredDF = batchDF.filter(col("value") > currentThreshold)

        if (!filteredDF.isEmpty) {
          println(s"Values above threshold in this batch:")
          filteredDF.show()
        }
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it.
    // For processComplexJsonStream, start a netcat session: nc -lk 12345
    // and paste JSON strings, e.g.:
    // {"id":"1","user":{"name":"John","email":"j@d.com"},"timestamp":"2025-07-18T10:00:00.000Z","payload":"some data"}
    // this is not a valid json
    // {"id":"2","user":{"name":"Jane"},"timestamp":"2025-07-18T10:01:00.000Z","payload":"more data"}

    // For customKeyValueSink, just run it.

    // For processWithDynamicConfig, create the file at src/main/resources/data/config/threshold.txt
    // with a number inside. Change the number while the stream is running to see the effect.
    // e.g., echo 50 > src/main/resources/data/config/threshold.txt

    processWithDynamicConfig()
  }
}
