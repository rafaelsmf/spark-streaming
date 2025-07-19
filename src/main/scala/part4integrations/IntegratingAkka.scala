package part4integrations

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, SparkSession}
import common._

object IntegratingAkka {

  val spark = SparkSession.builder()
    .appName("Integrating Akka")
    .master("local[2]")
    .getOrCreate()

  // foreachBatch
  // receiving system is on another JVM

  import spark.implicits._

  def writeCarsToAkka(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        batch.foreachPartition { cars: Iterator[Car] =>
          // this code is run by a single executor

          val system = ActorSystem(s"SourceSystem$batchId", ConfigFactory.load("akkaconfig/remoteActors"))
          val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

          // send all the data
          cars.foreach(car => entryPoint ! car)
        }
      }
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Two-Way Communication with Backpressure
  //    Modify the Akka sink to send an acknowledgment for every 100 cars it receives.
  //    The Spark `foreachPartition` logic should wait for this acknowledgment before sending the next 100 cars.
  //    The acknowledgment should contain the number of cars processed in that batch.
  def writeCarsWithBackpressure(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        batch.foreachPartition { cars: Iterator[Car] =>
          import akka.pattern.ask
          import akka.util.Timeout
          import scala.concurrent.duration._
          import scala.concurrent.Await

          implicit val timeout = Timeout(5 seconds)
          val system = ActorSystem(s"SourceSystem$batchId", ConfigFactory.load("akkaconfig/remoteActors"))
          val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

          val carBuffer = cars.toSeq
          carBuffer.sliding(100, 100).foreach { carBatch =>
            println(s"Sending batch of ${carBatch.size} cars")
            val ackFuture = entryPoint ? carBatch
            val ack = Await.result(ackFuture, 5 seconds)
            println(s"Received ack: $ack")
          }
        }
      }
      .start()
      .awaitTermination()
  }

  // 2. Fault-Tolerant Akka Sink using ForeachWriter
  //    Implement a custom `ForeachWriter` that connects to the Akka system.
  //    It should handle connection errors with retries (e.g., exponential backoff).
  //    The writer should send data and close the connection gracefully.
  class AkkaForeachWriter extends org.apache.spark.sql.ForeachWriter[Car] {
    val config = ConfigFactory.load("akkaconfig/remoteActors")
    var system: ActorSystem = _
    var entryPoint: ActorRef = _

    def open(partitionId: Long, epochId: Long): Boolean = {
      // This method is called once per partition/epoch to open the connection.
      println(s"Opening connection for partition $partitionId, epoch $epochId")
      try {
        system = ActorSystem(s"SourceSystem-$partitionId-$epochId", config)
        entryPoint = system.actorOf(Props[EntryPoint](), "entrypoint") // Assuming EntryPoint is local for simplicity
        true
      } catch {
        case e: Exception =>
          e.printStackTrace()
          false // Failed to open
      }
    }

    def process(value: Car): Unit = {
      // This method is called for each record.
      // Add retry logic here if needed.
      entryPoint ! value
    }

    def close(errorOrNull: Throwable): Unit = {
      // This method is called to close the connection.
      println(s"Closing connection. Error: $errorOrNull")
      if (system != null) {
        system.terminate()
      }
    }
  }

  def writeCarsWithForeachWriter(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreach(new AkkaForeachWriter)
      .start()
      .awaitTermination()
  }

  // 3. Aggregate and Send
  //    Instead of sending individual cars, aggregate them in each micro-batch by origin
  //    and send only the counts to the Akka system.
  def aggregateAndSend(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        val countsByOrigin = batch.groupByKey(_.Origin).count().collect()

        val system = ActorSystem(s"SourceSystem$batchId", ConfigFactory.load("akkaconfig/remoteActors"))
        val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

        entryPoint ! s"--- Batch $batchId ---"
        countsByOrigin.foreach { case (origin, count) =>
          entryPoint ! s"Origin: $origin, Count: $count"
        }
      }
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it and start the ReceiverSystem.
    // writeCarsWithBackpressure()
    // writeCarsWithForeachWriter()
     aggregateAndSend()
  }
}

object ReceiverSystem {
  implicit val actorSystem = ActorSystem("ReceiverSystem", ConfigFactory.load("akkaconfig/remoteActors").getConfig("remoteSystem"))
  implicit val actorMaterializer = ActorMaterializer()

  class Destination extends Actor with ActorLogging {
    override def receive = {
      case m => log.info(m.toString)
    }
  }

  object EntryPoint {
    def props(destination: ActorRef) = Props(new EntryPoint(destination))
  }

  class EntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive = {
      case cars: Seq[Car] @unchecked =>
        log.info(s"Received a batch of ${cars.size} cars.")
        cars.foreach(destination ! _)
        sender() ! s"Ack for ${cars.size} cars"
      case m =>
        log.info(s"Received $m")
        destination ! m
    }
  }

  def main(args: Array[String]): Unit = {
    val source = Source.actorRef[Any](bufferSize = 1000, overflowStrategy = OverflowStrategy.dropHead)
    val sink = Sink.foreach[Any](println)
    val runnableGraph = source.to(sink)
    val destination: ActorRef = runnableGraph.run()

    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination), "entrypoint")
    println("ReceiverSystem is running.")
  }
}
