package part4integrations

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object IntegratingKafkaDStreams {

  val spark = SparkSession.builder()
    .appName("Spark DStreams + Kafka")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val kafkaParams: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.serializer" -> classOf[StringSerializer], // send data to kafka
    "value.serializer" -> classOf[StringSerializer],
    "key.deserializer" -> classOf[StringDeserializer], // receiving data from kafka
    "value.deserializer" -> classOf[StringDeserializer],
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object]
  )

  val kafkaTopic = "rockthejvm"

  def readFromKafka() = {
    val topics = Array(kafkaTopic)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      /*
       Distributes the partitions evenly across the Spark cluster.
       Alternatives:
       - PreferBrokers if the brokers and executors are in the same cluster
       - PreferFixed
      */
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams + ("group.id" -> "group1"))
      /*
        Alternative
        - SubscribePattern allows subscribing to topics matching a pattern
        - Assign - advanced; allows specifying offsets and partitions per topic
       */
    )

    val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka() = {
    val inputData = ssc.socketTextStream("localhost", 12345)

    // transform data
    val processedData = inputData.map(_.toUpperCase())

    processedData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        // inside this lambda, the code is run by a single executor

        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaParams.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        // producer can insert records into the Kafka topics
        // available on this executor
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach { value =>
          val message = new ProducerRecord[String, String](kafkaTopic, null, value)
          // feed the message into the Kafka topic
          producer.send(message)
        }

        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Manual Offset Management
  // Instead of using group.id and auto-commit, manage offsets manually to achieve more precise processing guarantees.
  // After processing each batch, commit the offsets back to Kafka.
  def manualOffsetManagement(): Unit = {
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParams)
    )

    kafkaDStream.foreachRDD { rdd =>
      // Get the offset ranges for the RDD
      val offsetRanges = rdd.asInstanceOf[org.apache.spark.streaming.kafka010.HasOffsetRanges].offsetRanges

      // Process the data
      println("Processing data...")
      rdd.map(record => s"Key: ${record.key()}, Value: ${record.value()}").foreach(println)

      // Commit the offsets back to Kafka
      println("Committing offsets...")
      kafkaDStream.asInstanceOf[org.apache.spark.streaming.kafka010.CanCommitOffsets].commitAsync(offsetRanges)
      println("Offsets committed.")
    }

    ssc.start()
    ssc.awaitTermination()
  }

  // 2. Exactly-Once Semantics with a Transactional Sink
  // Read from a Kafka topic and write to another topic transactionally.
  // This ensures that the data is processed and written exactly once, even in the face of failures.
  def exactlyOnceSemantics(): Unit = {
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParams)
    )

    kafkaDStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[org.apache.spark.streaming.kafka010.HasOffsetRanges].offsetRanges

      val producerProps = new util.HashMap[String, Object]()
      kafkaParams.foreach { case (k, v) => producerProps.put(k, v) }
      producerProps.put("transactional.id", s"spark-dstream-txn-${rdd.id}")
      producerProps.put("enable.idempotence", "true")

      val producer = new KafkaProducer[String, String](producerProps)

      try {
        producer.beginTransaction()
        println(s"Transaction ${producerProps.get("transactional.id")} started.")

        rdd.foreachPartition { partition =>
          partition.foreach { record =>
            val newRecord = new ProducerRecord[String, String]("rockthejvm-output", record.key(), s"PROCESSED: ${record.value()}")
            producer.send(newRecord)
          }
        }

        // The sendOffsetsToTransaction method is not directly available in the standard producer API used this way.
        // This pattern is more straightforward with Structured Streaming.
        // For DStreams, committing offsets after a successful transaction is the common pattern.
        producer.commitTransaction()
        println(s"Transaction ${producerProps.get("transactional.id")} committed.")
        kafkaDStream.asInstanceOf[org.apache.spark.streaming.kafka010.CanCommitOffsets].commitAsync(offsetRanges)

      } catch {
        case e: Exception =>
          println(s"Aborting transaction ${producerProps.get("transactional.id")}")
          producer.abortTransaction()
          throw e
      } finally {
        producer.close()
      }
    }

    ssc.checkpoint("checkpoints") // Required for some transactional guarantees
    ssc.start()
    ssc.awaitTermination()
  }

  // 3. Rate Limiting Kafka Consumption
  // Use Spark's built-in configuration to limit the rate at which data is read from Kafka partitions.
  def rateLimitedConsumption(): Unit = {
    spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "10") // 10 messages per second per partition

    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParams + ("group.id" -> "rateGroup"))
    )

    kafkaDStream.map(record => (record.partition(), record.offset(), record.value()))
      .print()

    ssc.start()
    ssc.awaitTermination()
  }

  // 4. Handling Consumer Rebalance
  // Implement a ConsumerRebalanceListener to log when partitions are assigned or revoked from the consumer.
  // This is useful for diagnostics and managing state associated with partitions.
  def withRebalanceListener(): Unit = {
    import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
    import org.apache.kafka.common.TopicPartition
    import java.util

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParams + ("group.id" -> "rebalanceGroup"))
      .setConsumerRebalanceListener(new ConsumerRebalanceListener {
        override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
          println(s"Partitions revoked: $partitions")
        }
        override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
          println(s"Partitions assigned: $partitions")
        }
      })

    val kafkaDStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, consumerStrategy)
    kafkaDStream.map(_.value).print()
    ssc.start()
    ssc.awaitTermination()
  }

  // 5. Joining Kafka Stream with Dynamic Broadcast Variable
  // Enrich a stream of events from Kafka with reference data that can be updated periodically.
  // The reference data is broadcasted to all executors and can be swapped out without restarting the stream.
  def joinWithDynamicBroadcast(): Unit = {
    ssc.checkpoint("checkpoints")

    // Reference data (e.g., user details) - could be read from a file or DB
    var referenceData = spark.sparkContext.broadcast(Map("key1" -> "info1", "key2" -> "info2"))

    // A separate thread to update the broadcast variable every 20 seconds
    new Thread(() => {
      var count = 0
      while (true) {
        Thread.sleep(20000)
        referenceData.unpersist() // Unpersist the old broadcast variable
        referenceData = spark.sparkContext.broadcast(Map("key1" -> s"new_info1_$count", "key2" -> s"new_info2_$count"))
        println(s"Updated broadcast variable. Count: $count")
        count += 1
      }
    }).start()

    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopic), kafkaParams + ("group.id" -> "broadcastGroup"))
    )

    val enrichedStream = kafkaDStream.transform { rdd =>
      rdd.map { record =>
        val key = record.key()
        val value = record.value()
        val enrichedValue = referenceData.value.getOrElse(key, "N/A")
        (key, value, enrichedValue)
      }
    }

    enrichedStream.print()
    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it.
    // You will need a running Kafka instance.
    // For most exercises, you'll need a producer sending data to the 'rockthejvm' topic.
    // e.g., kafka-console-producer.sh --broker-list localhost:9092 --topic rockthejvm
    // manualOffsetManagement()
    // exactlyOnceSemantics()
    // rateLimitedConsumption()
    // withRebalanceListener()
    joinWithDynamicBroadcast()
  }
}
