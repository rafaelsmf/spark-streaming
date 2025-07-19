package part4integrations

import org.apache.spark.sql.{Dataset, SparkSession}
import common._

object IntegratingJDBC {

  val spark = SparkSession.builder()
    .appName("Integrating JDBC")
    .master("local[2]")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  import spark.implicits._

  def writeStreamToPostgres() = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        // each executor can control the batch
        // batch is a STATIC Dataset/DataFrame

        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars")
          .save()
      }
      .start()
      .awaitTermination()

  }

  /**
    * Extra Exercises
    */

  // 1. JDBC Sink with Transactional Guarantees
  //    Implement a custom `ForeachWriter` to write to a JDBC sink.
  //    The writer should manage its own connection and perform writes within a transaction.
  //    If any part of the batch fails, the entire transaction for that partition should be rolled back.
  class TransactionalJdbcWriter extends org.apache.spark.sql.ForeachWriter[Car] {
    import java.sql.{Connection, DriverManager, PreparedStatement}

    var connection: Connection = _
    var statement: PreparedStatement = _

    override def open(partitionId: Long, epochId: Long): Boolean = {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, user, password)
      connection.setAutoCommit(false) // Start transaction
      statement = connection.prepareStatement("INSERT INTO public.cars (\"Name\", \"Horsepower\", \"Origin\") VALUES (?, ?, ?)")
      true
    }

    override def process(car: Car): Unit = {
      statement.setString(1, car.Name)
      car.Horsepower.foreach(hp => statement.setLong(2, hp))
      statement.setString(3, car.Origin)
      statement.addBatch()
    }

    override def close(errorOrNull: Throwable): Unit = {
      if (errorOrNull == null) {
        try {
          statement.executeBatch()
          connection.commit() // Commit transaction
        } catch {
          case e: Exception =>
            println(s"Error during batch execution, rolling back transaction.")
            connection.rollback()
            throw e
        }
      } else {
        println(s"Error processing data, rolling back transaction.")
        connection.rollback()
      }
      connection.close()
    }
  }

  def writeWithTransactions(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS.writeStream
      .foreach(new TransactionalJdbcWriter)
      .start()
      .awaitTermination()
  }

  // 2. Dynamic Schema Evolution
  //    In `foreachBatch`, before writing, inspect the DataFrame's schema.
  //    Compare it to the target table's schema and issue ALTER TABLE commands for new columns.
  def writeWithSchemaEvolution(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    carsDF.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, _: Long) =>
        import java.sql.DriverManager

        val conn = DriverManager.getConnection(url, user, password)
        try {
          val tableMetadata = conn.getMetaData.getColumns(null, "public", "cars_evolution", null)
          val existingColumns = new scala.collection.mutable.HashSet[String]()
          while (tableMetadata.next()) {
            existingColumns.add(tableMetadata.getString("COLUMN_NAME").toLowerCase)
          }

          batch.schema.fields.foreach { field =>
            if (!existingColumns.contains(field.name.toLowerCase)) {
              println(s"Column ${field.name} not found in table. Adding it.")
              val alterStatement = conn.createStatement()
              // This is a simplified type mapping. A real implementation needs more robust mapping.
              val columnType = field.dataType match {
                case org.apache.spark.sql.types.LongType => "BIGINT"
                case org.apache.spark.sql.types.DoubleType => "DOUBLE PRECISION"
                case _ => "TEXT"
              }
              alterStatement.executeUpdate(s"""ALTER TABLE public.cars_evolution ADD COLUMN "${field.name}" $columnType""")
              alterStatement.close()
            }
          }
        } finally {
          conn.close()
        }

        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars_evolution")
          .mode(org.apache.spark.sql.SaveMode.Append)
          .save()
      }
      .start()
      .awaitTermination()
  }

  // 3. Upsert with a Staging Table
  //    Implement an efficient "upsert" using a staging table.
  //    1. Write the batch to a temporary staging table.
  //    2. Execute a single SQL statement to UPDATE existing rows and INSERT new rows from the staging table.
  //    3. Truncate the staging table.
  def writeWithUpsert(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    carsDF.writeStream
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, _: Long) =>
        // 1. Write to staging table (overwrite)
        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.cars_staging")
          .mode(org.apache.spark.sql.SaveMode.Overwrite) // Overwrite staging table with new batch
          .save()

        // 2. Perform upsert from staging to main table
        import java.sql.DriverManager
        val conn = DriverManager.getConnection(url, user, password)
        try {
          val upsertSql =
            """
              |INSERT INTO public.cars_main ("Name", "Horsepower", "Origin")
              |SELECT "Name", "Horsepower", "Origin" FROM public.cars_staging
              |ON CONFLICT ("Name") DO UPDATE
              |SET
              |  "Horsepower" = EXCLUDED."Horsepower",
              |  "Origin" = EXCLUDED."Origin";
            """.stripMargin
          val statement = conn.createStatement()
          statement.executeUpdate(upsertSql)
          statement.close()
        } finally {
          conn.close()
        }
      }
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // Before running, ensure you have a PostgreSQL instance and have created the tables.
    // CREATE TABLE public.cars ("Name" TEXT, "Horsepower" BIGINT, "Origin" TEXT);
    // CREATE TABLE public.cars_evolution ("Name" TEXT);
    // CREATE TABLE public.cars_main ("Name" TEXT PRIMARY KEY, "Horsepower" BIGINT, "Origin" TEXT);
    // CREATE TABLE public.cars_staging ("Name" TEXT, "Horsepower" BIGINT, "Origin" TEXT);
    writeWithUpsert()
  }
}
