package part4integrations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._

object IntegratingKafka {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  def readFromKafka() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key", "Name as value")

    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints") // without checkpoints the writing to Kafka will fail
      .start()
      .awaitTermination()
  }

  /**
    * Exercise: write the whole cars data structures to Kafka as JSON.
    * Use struct columns an the to_json function.
    */
  def writeCarsToKafka() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsJsonKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
    )

    carsJsonKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Process Kafka Avro data
  //    Use the spark-avro library to read Avro-serialized data from a Kafka topic.
  //    You'll need to add the dependency: org.apache.spark:spark-avro_2.12:3.0.2
  //    The Avro schema will be provided as a JSON string.
  def readAvroFromKafka(): Unit = {
    val avroSchema =
      """
        |{
        |  "type": "record",
        |  "name": "Car",
        |  "namespace": "common",
        |  "fields": [
        |    {"name": "Name", "type": "string"},
        |    {"name": "Horsepower", "type": ["long", "null"]},
        |    {"name": "Origin", "type": "string"}
        |  ]
        |}
        |""".stripMargin

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cars_avro")
      .load()

    // Use the from_avro function from the spark-avro library
    import org.apache.spark.sql.avro.functions._
    val carsDF = kafkaDF.select(from_avro(col("value"), avroSchema).as("car")).select("car.*")

    carsDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // 2. Write to Kafka with transactional guarantees
  //    Use foreachBatch to write to Kafka, but ensure that the write is transactional.
  //    This means the data is either all written or not at all.
  //    Kafka producers can be configured for idempotence and transactions.
  def writeToKafkaTransactionally(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    carsDF.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, batchId: Long) =>
        val kafkaProducerProps = Map(
          "bootstrap.servers" -> "localhost:9092",
          "transactional.id" -> s"spark-streaming-transaction-$batchId", // Unique transactional id
          "enable.idempotence" -> "true" // Required for transactions
        )

        // Select data and format for Kafka
        val kafkaBatch = batch.select(
          col("Name").as("key"),
          to_json(struct(col("*"))).as("value")
        )

        // Write to Kafka using the producer API within a transaction
        kafkaBatch.write
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "cars_transactional")
          .options(kafkaProducerProps.map { case (k, v) => (s"kafka.$k", v) })
          .save()
      }
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

  // 3. Kafka stream to stream join with another source (e.g., a file source)
  //    Read car sales events from Kafka: (car_name, price, timestamp)
  //    Join this stream with a static/streaming DataFrame of car details (Name, Origin)
  //    to enrich the sales event with the car's origin.
  def kafkaStreamToFileJoin(): Unit = {
    val salesSchema = org.apache.spark.sql.types.StructType(Array(
      org.apache.spark.sql.types.StructField("car_name", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("price", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField("timestamp", org.apache.spark.sql.types.TimestampType)
    ))

    val salesStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "car_sales")
      .load()
      .select(from_json(col("value").cast("string"), salesSchema).as("sale"))
      .selectExpr("sale.car_name as Name", "sale.price", "sale.timestamp")
      .withWatermark("timestamp", "2 minutes")

    val carDetails = spark.read
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .select("Name", "Origin")

    val enrichedSales = salesStream.join(carDetails, "Name")

    enrichedSales.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // To run an exercise, you'll need a Kafka instance.
    // For readAvroFromKafka, you need a producer that sends Avro-serialized data.
    // For writeToKafkaTransactionally, you can check the topic for messages.
    // For kafkaStreamToFileJoin, you need a producer for car_sales topic.
    // e.g., {"car_name":"Ford Torino","price":25000.0,"timestamp":"2023-01-01T12:00:00Z"}
    kafkaStreamToFileJoin()
  }
}
