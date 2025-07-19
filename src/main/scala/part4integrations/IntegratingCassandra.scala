package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import common._

object IntegratingCassandra {

  val spark = SparkSession.builder()
    .appName("Integrating Cassandra")
    .master("local[2]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  // for noisy logs
  // spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def writeStreamToCassandraInBatches() = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // save this batch to Cassandra in a single table write
        batch
          .select(col("Name"), col("Horsepower"))
          .write
          .cassandraFormat("cars", "public") // type enrichment
          .mode(SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  class CarCassandraForeachWriter extends ForeachWriter[Car] {

    /*
      - on every batch, on every partition `partitionId`
        - on every "epoch" = chunk of data
          - call the open method; if false, skip this chunk
          - for each entry in this chunk, call the process method
          - call the close method either at the end of the chunk or with an error if it was thrown
     */

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      println("Open connection")
      true
    }

    override def process(car: Car): Unit = {
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |insert into $keyspace.$table("Name", "Horsepower")
             |values ('${car.Name}', ${car.Horsepower.orNull})
           """.stripMargin)
      }
    }

    override def close(errorOrNull: Throwable): Unit = println("Closing connection")

  }

  def writeStreamToCassandra() = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreach(new CarCassandraForeachWriter)
      .start()
      .awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Upsert with Rate Limiting
  //    Use foreachBatch to perform an "upsert" operation (insert if not exists, update if exists).
  //    Incorporate a rate limiter (e.g., Guava's RateLimiter) inside foreachPartition
  //    to control the write throughput to Cassandra.
  def upsertWithRateLimiting(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        println(s"Processing batch $batchId")
        batch.foreachPartition { cars: Iterator[Car] =>
          // This code runs on the executor.
          // A single RateLimiter instance per partition.
          val rateLimiter = com.google.common.util.concurrent.RateLimiter.create(10) // 10 permits per second
          val connector = CassandraConnector(spark.sparkContext.getConf)

          cars.foreach { car =>
            rateLimiter.acquire() // This will block until a permit is available
            connector.withSessionDo { session =>
              // A simple upsert is just an INSERT statement in Cassandra.
              // For a more complex one, you might read first, but that's less efficient.
              session.execute(
                s"""
                   |INSERT INTO public.cars ("Name", "Horsepower", "Origin")
                   |VALUES ('${car.Name}', ${car.Horsepower.orNull}, '${car.Origin}')
                 """.stripMargin)
            }
          }
        }
      }
      .start()
      .awaitTermination()
  }

  // 2. Dynamic Table Creation per Origin
  //    In foreachBatch, for each car's Origin, check if a table named `cars_[origin]` exists.
  //    If not, create it dynamically. Then, write the car data to its origin-specific table.
  //    e.g., a car from "USA" goes to "cars_usa".
  def dynamicTableCreation(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        val origins = batch.select("Origin").distinct().as[String].collect()
        val connector = CassandraConnector(spark.sparkContext.getConf)

        // Create tables on the driver if they don't exist.
        // This is not idempotent and should be done carefully in production.
        connector.withSessionDo { session =>
          origins.foreach { origin =>
            val tableName = s"cars_${origin.toLowerCase.replaceAll("[^a-z0-9]", "")}"
            println(s"Ensuring table public.$tableName exists.")
            session.execute(
              s"""
                 |CREATE TABLE IF NOT EXISTS public.$tableName (
                 |  "Name" text PRIMARY KEY,
                 |  "Horsepower" bigint,
                 |  "Origin" text
                 |);
               """.stripMargin)
          }
        }

        // Write data to the respective tables
        batch.foreachPartition { cars: Iterator[Car] =>
          val partitionConnector = CassandraConnector(spark.sparkContext.getConf)
          cars.foreach { car =>
            val tableName = s"cars_${car.Origin.toLowerCase.replaceAll("[^a-z0-9]", "")}"
            partitionConnector.withSessionDo { session =>
              session.execute(
                s"""
                   |INSERT INTO public.$tableName ("Name", "Horsepower", "Origin")
                   |VALUES ('${car.Name}', ${car.Horsepower.orNull}, '${car.Origin}')
                 """.stripMargin)
            }
          }
        }
      }
      .start()
      .awaitTermination()
  }

  // 3. Read-Modify-Write with Consistency Level Control
  //    Use a ForeachWriter to read a record, increment a "view_count" column, and write it back.
  //    Allow specifying different consistency levels for reads and writes.
  class ReadModifyWriteCars(readConsistency: String, writeConsistency: String) extends ForeachWriter[Car] {
    val keyspace = "public"
    val table = "cars_views"
    val connector = CassandraConnector(spark.sparkContext.getConf)

    override def open(partitionId: Long, epochId: Long): Boolean = {
      // Create table if not exists
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |CREATE TABLE IF NOT EXISTS $keyspace.$table (
             |  "Name" text PRIMARY KEY,
             |  "view_count" counter
             |);
           """.stripMargin)
      }
      true
    }

    override def process(car: Car): Unit = {
      // Counters are a special case in Cassandra. A simple UPDATE increments them.
      // No need to read first. This makes the operation atomic and efficient.
      connector.withSessionDo { session =>
        val update = session.prepare(s"""UPDATE $keyspace.$table SET "view_count" = "view_count" + 1 WHERE "Name" = ?""")
        session.execute(update.bind(car.Name).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.valueOf(writeConsistency)))
      }
    }

    override def close(errorOrNull: Throwable): Unit = {}
  }

  def readModifyWrite(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreach(new ReadModifyWriteCars("ONE", "QUORUM"))
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // Before running, ensure you have a Cassandra instance and have created the 'public' keyspace.
    // cqlsh> CREATE KEYSPACE public WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    // upsertWithRateLimiting()
    // dynamicTableCreation()
    readModifyWrite()
  }
}
