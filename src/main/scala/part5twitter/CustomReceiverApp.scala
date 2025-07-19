package part5twitter

import java.net.Socket

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.{Future, Promise}
import scala.io.Source

class CustomSocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture = socketPromise.future

  // called asynchronously
  override def onStart(): Unit = {
    val socket = new Socket(host, port)

    // run on another thread
    Future {
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line)) // store makes this string available to Spark
    }

    socketPromise.success(socket)
  }

  // called asynchronously
  override def onStop(): Unit = socketFuture.foreach(socket => socket.close())
}

object CustomReceiverApp {

  val spark = SparkSession.builder()
    .appName("Custom receiver app")
    .master("local[*]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  /**
    * Extra Exercises
    */

  // 1. Rate-Limited Receiver
  //    Modify the CustomSocketReceiver to be rate-limited. It should only `store()` a certain number of messages per second,
  //    configurable upon creation. This involves internal state management and timing within the receiver's thread.
  class RateLimitedSocketReceiver(host: String, port: Int, messagesPerSecond: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    import scala.concurrent.ExecutionContext.Implicits.global
    import com.google.common.util.concurrent.RateLimiter

    val socketPromise: Promise[Socket] = Promise[Socket]()
    val socketFuture = socketPromise.future

    override def onStart(): Unit = {
      val socket = new Socket(host, port)
      val rateLimiter = RateLimiter.create(messagesPerSecond)

      Future {
        Source.fromInputStream(socket.getInputStream)
          .getLines()
          .foreach { line =>
            rateLimiter.acquire() // Blocks until a permit is available
            store(line)
          }
      }
      socketPromise.success(socket)
    }

    override def onStop(): Unit = socketFuture.foreach(_.close())
  }

  // 2. Auto-Reconnect Receiver
  //    Make the receiver resilient to network failures. If the connection to the socket is lost,
  //    it should attempt to reconnect automatically with an exponential backoff strategy.
  class AutoReconnectReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    import scala.concurrent.ExecutionContext.Implicits.global
    import java.util.concurrent.atomic.AtomicBoolean

    private val running = new AtomicBoolean(true)

    override def onStart(): Unit = {
      new Thread("Socket Reconnect Thread") {
        override def run(): Unit = {
          var backoffMillis = 500
          while (running.get() && !isStopped()) {
            try {
              val socket = new Socket(host, port)
              reportError("Connection established.", null)
              backoffMillis = 500 // Reset backoff on successful connection
              val reader = Source.fromInputStream(socket.getInputStream).getLines()
              while(reader.hasNext && !isStopped()) {
                store(reader.next())
              }
              socket.close()
            } catch {
              case e: Exception =>
                reportError(s"Connection failed. Reconnecting in $backoffMillis ms.", e)
                Thread.sleep(backoffMillis)
                backoffMillis = Math.min(backoffMillis * 2, 10000) // Exponential backoff up to 10s
            }
          }
        }
      }.start()
    }

    override def onStop(): Unit = {
      running.set(false)
    }
  }

  // 3. Receiver with Dynamic Control Port
  //    Implement a control mechanism. Besides the data socket, the receiver should listen on a second "control" port.
  //    Sending "pause" should temporarily stop storing data, and "resume" should continue.
  class ControllableReceiver(dataHost: String, dataPort: Int, controlPort: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    import scala.concurrent.ExecutionContext.Implicits.global
    import java.util.concurrent.atomic.AtomicBoolean
    import java.net.ServerSocket

    private val paused = new AtomicBoolean(false)

    override def onStart(): Unit = {
      // Data thread
      new Thread("Data Thread") {
        override def run(): Unit = {
          try {
            val socket = new Socket(dataHost, dataPort)
            Source.fromInputStream(socket.getInputStream).getLines().foreach { line =>
              if (!paused.get()) {
                store(line)
              }
            }
            socket.close()
          } catch {
            case e: Exception => reportError("Data connection failed.", e)
          }
        }
      }.start()

      // Control thread
      new Thread("Control Thread") {
        override def run(): Unit = {
          try {
            val serverSocket = new ServerSocket(controlPort)
            while(!isStopped()) {
              val controlSocket = serverSocket.accept()
              val command = Source.fromInputStream(controlSocket.getInputStream).getLines().next()
              command.toLowerCase match {
                case "pause" =>
                  reportError("Pausing receiver.", null)
                  paused.set(true)
                case "resume" =>
                  reportError("Resuming receiver.", null)
                  paused.set(false)
                case _ => reportError(s"Unknown command: $command", null)
              }
              controlSocket.close()
            }
            serverSocket.close()
          } catch {
            case e: Exception => reportError("Control connection failed.", e)
          }
        }
      }.start()
    }

    override def onStop(): Unit = {}
  }


  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment the appropriate stream and start a netcat session.
    // For RateLimitedSocketReceiver: nc -lk 12345
    // val dataStream: DStream[String] = ssc.receiverStream(new RateLimitedSocketReceiver("localhost", 12345, 1))

    // For AutoReconnectReceiver: nc -lk 12345. Try stopping and restarting the netcat.
    // val dataStream: DStream[String] = ssc.receiverStream(new AutoReconnectReceiver("localhost", 12345))

    // For ControllableReceiver:
    // Data: nc -lk 12345
    // Control: nc localhost 12346 (then type 'pause' or 'resume' and enter)
    val dataStream: DStream[String] = ssc.receiverStream(new ControllableReceiver("localhost", 12345, 12346))

    dataStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
