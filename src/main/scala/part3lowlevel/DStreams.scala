package part3lowlevel

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._

object DStreams {

  val spark = SparkSession.builder()
    .appName("DStreams")
    .master("local[2]")
    .getOrCreate()

  /*
    Spark Streaming Context = entry point to the DStreams API
    - needs the spark context
    - a duration = batch interval
   */
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /*
    - define input sources by creating DStreams
    - define transformations on DStreams
    - call an action on DStreams
    - start ALL computations with ssc.start()
      - no more computations can be added
    - await termination, or stop the computation
      - you cannot restart the ssc
   */

  def readFromSocket() = {
    val socketStream: DStream[String] = ssc.socketTextStream("localhost", 12345)

    // transformation = lazy
    val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

    // action
//    wordsStream.print()
    wordsStream.saveAsTextFiles("src/main/resources/data/words/") // each folder = RDD = batch, each file = a partition of the RDD

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() => {
      Thread.sleep(5000)

      val path = "src/main/resources/data/stocks"
      val dir = new File(path) // directory where I will store a new file
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStocks$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
        """.stripMargin.trim)

      writer.close()
    }).start()
  }

  def readFromFile() = {
    createNewFile() // operates on another thread

    // defined DStream
    val stocksFilePath = "src/main/resources/data/stocks"

    /*
      ssc.textFileStream monitors a directory for NEW FILES
     */
    val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformations
    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }

    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Custom Receiver for JSON Data
  //    Implement a custom receiver that listens on a socket for JSON strings representing orders.
  //    The receiver should parse the JSON and push a DStream of `Order` case classes.
  //    Handle JSON parsing errors gracefully.
  case class Order(orderId: String, customerId: String, amount: Double, timestamp: Long)
  class JsonOrderReceiver(host: String, port: Int) extends org.apache.spark.streaming.receiver.Receiver[String](org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2) {
    import java.net.{ServerSocket, Socket}
    import java.io.BufferedReader, java.io.InputStreamReader

    private var serverSocket: ServerSocket = _

    def onStart(): Unit = {
      serverSocket = new ServerSocket(port)
      new Thread("Socket Receiver") {
        override def run(): Unit = { receive() }
      }.start()
    }

    def onStop(): Unit = {
      if (serverSocket != null) {
        serverSocket.close()
        serverSocket = null
      }
    }

    private def receive(): Unit = {
      try {
        while (!isStopped()) {
          val clientSocket = serverSocket.accept()
          val reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
          var line: String = null
          while ({line = reader.readLine(); line != null} && !isStopped()) {
            store(line)
          }
          clientSocket.close()
        }
      } catch {
        case e: java.net.SocketException => // Expected on stop
        case e: Throwable => restart("Error receiving data", e)
      }
    }
  }

  def customReceiverExercise(): Unit = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats

    val jsonStream = ssc.receiverStream(new JsonOrderReceiver("localhost", 12345))

    val ordersStream: DStream[Order] = jsonStream.flatMap { jsonString =>
      try {
        Some(parse(jsonString).extract[Order])
      } catch {
        case e: Exception =>
          println(s"Failed to parse JSON: $jsonString")
          None
      }
    }

    ordersStream.foreachRDD { rdd =>
      if (!rdd.isEmpty) {
        println("--- New Batch ---")
        rdd.take(5).foreach(println)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  // 2. Fault-Tolerant File Processing with Dead-Letter Queue
  //    Read a directory of CSV files. Some lines may be malformed (e.g., wrong number of columns).
  //    Valid lines should be converted to a DStream of `Stock` objects.
  //    Malformed lines should be sent to a separate DStream (a "dead-letter" stream) and written to a separate error directory.
  def faultTolerantFileProcessing(): Unit = {
    val stocksFilePath = "src/main/resources/data/stocks"
    val errorPath = "src/main/resources/data/errorStocks"

    val textStream = ssc.textFileStream(stocksFilePath)

    val parsedStream = textStream.map { line =>
      val tokens = line.split(",")
      if (tokens.length == 3) {
        try {
          val dateFormat = new SimpleDateFormat("MMM d yyyy")
          val company = tokens(0)
          val date = new Date(dateFormat.parse(tokens(1)).getTime)
          val price = tokens(2).toDouble
          Right(Stock(company, date, price))
        } catch {
          case e: Exception => Left(s"Error parsing line: $line, Exception: ${e.getMessage}")
        }
      } else {
        Left(s"Malformed line: $line")
      }
    }

    val validStocksStream = parsedStream.filter(_.isRight).map(_.right.get)
    val deadLetterStream = parsedStream.filter(_.isLeft).map(_.left.get)

    validStocksStream.print()
    deadLetterStream.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty) {
        rdd.saveAsTextFile(s"$errorPath/errors_${time.milliseconds}")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  // 3. Dynamic Directory Monitoring
  //    Create a DStream that monitors a "control" socket.
  //    This socket will receive paths to new directories that need to be monitored for files.
  //    The main DStream should then start monitoring these new directories as they are added.
  //    This is very advanced and might require managing multiple DStreams or a custom input source.
  def dynamicDirectoryMonitoring(): Unit = {
    // This is a conceptual example. A robust implementation is complex.
    // We can use a mutable collection of DStreams, but adding new streams after ssc.start() is not allowed.
    // A better approach is a custom receiver that manages file monitoring internally.

    // Simplified approach: Recreate StreamingContext (not ideal for production)
    // or use a single DStream that reads from a "meta-directory" where new files are symlinks to files in target directories.

    // Let's simulate with a single textFileStream and a control socket that writes files to the monitored directory.
    val monitoredPath = "src/main/resources/data/dynamic"
    new File(monitoredPath).mkdirs()

    val controlSocket = ssc.socketTextStream("localhost", 9998)
    controlSocket.foreachRDD { rdd =>
      rdd.foreach { content =>
        // In a real scenario, 'content' would be a path to a file to copy/move.
        // Here, we just write the content to a new file in the monitored directory.
        val timestamp = System.currentTimeMillis()
        val writer = new FileWriter(new File(s"$monitoredPath/file_$timestamp.txt"))
        writer.write(content)
        writer.close()
      }
    }

    val fileStream = ssc.textFileStream(monitoredPath)
    fileStream.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it.
    // For customReceiverExercise:
    // Start this app, then in a terminal: nc -lk 12345
    // Then paste JSON: {"orderId":"1","customerId":"custA","amount":123.45,"timestamp":1640995200000}
    // customReceiverExercise()

    // For faultTolerantFileProcessing:
    // Create files in src/main/resources/data/stocks with both valid and invalid lines.
    // e.g., GOOG,May 1 2022,2300.50
    // e.g., MSFT,invalid-date,300.0
    // e.g., AMZN,only-two-parts
    // faultTolerantFileProcessing()

    // For dynamicDirectoryMonitoring:
    // Start this app, then in a terminal: nc -lk 9998
    // Type any text and press enter. A new file with that content will appear in the dynamic directory and be processed.
    dynamicDirectoryMonitoring()
  }
}
