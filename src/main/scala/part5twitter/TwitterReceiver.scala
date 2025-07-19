package part5twitter

import java.io.{OutputStream, PrintStream}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._

import scala.concurrent.Promise

class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def simpleStatusListener = new StatusListener {
    override def onStatus(status: Status): Unit = store(status)
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
    override def onStallWarning(warning: StallWarning): Unit = ()
    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  private def redirectSystemError() = System.setErr(new PrintStream(new OutputStream {
    override def write(b: Array[Byte]): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()
    override def write(b: Int): Unit = ()
  }))

  // run asynchronously
  override def onStart(): Unit = {
    redirectSystemError()

    val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
      .getInstance()
      .addListener(simpleStatusListener)
      .sample("en") // call the Twitter sample endpoint for English tweets

    twitterStreamPromise.success(twitterStream)
  }

  /**
    * Extra Exercises
    */

  // 1. Filtered Twitter Receiver with Dynamic Keywords
  // Extend the TwitterReceiver to accept a dynamic list of keywords to filter tweets.
  // The keywords can be updated at runtime through a control mechanism (e.g., a file watcher).
  class FilteredTwitterReceiver(initialKeywords: List[String]) extends Receiver[Status](StorageLevel.MEMORY_ONLY) {
    import scala.concurrent.ExecutionContext.Implicits.global
    import java.util.concurrent.atomic.AtomicReference
    
    private val keywords = new AtomicReference(initialKeywords.toArray)
    val twitterStreamPromise = Promise[TwitterStream]
    val twitterStreamFuture = twitterStreamPromise.future

    private def filteredStatusListener = new StatusListener {
      override def onStatus(status: Status): Unit = {
        val text = status.getText.toLowerCase
        val currentKeywords = keywords.get()
        if (currentKeywords.exists(keyword => text.contains(keyword.toLowerCase))) {
          store(status)
        }
      }
      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
      override def onStallWarning(warning: StallWarning): Unit = ()
      override def onException(ex: Exception): Unit = ex.printStackTrace()
    }

    def updateKeywords(newKeywords: List[String]): Unit = {
      keywords.set(newKeywords.toArray)
    }

    override def onStart(): Unit = {
      redirectSystemError()
      val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
        .getInstance()
        .addListener(filteredStatusListener)
        .sample("en")
      twitterStreamPromise.success(twitterStream)
    }

    override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
      twitterStream.cleanUp()
      twitterStream.shutdown()
    }
  }

  // 2. Rate-Limited Twitter Receiver
  // Implement a rate-limited version that controls the rate at which tweets are stored to Spark.
  // This helps prevent overwhelming the streaming application during high tweet volume periods.
  class RateLimitedTwitterReceiver(tweetsPerSecond: Int) extends Receiver[Status](StorageLevel.MEMORY_ONLY) {
    import scala.concurrent.ExecutionContext.Implicits.global
    import com.google.common.util.concurrent.RateLimiter
    
    val twitterStreamPromise = Promise[TwitterStream]
    val twitterStreamFuture = twitterStreamPromise.future
    private val rateLimiter = RateLimiter.create(tweetsPerSecond)

    private def rateLimitedStatusListener = new StatusListener {
      override def onStatus(status: Status): Unit = {
        rateLimiter.acquire()
        store(status)
      }
      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
      override def onStallWarning(warning: StallWarning): Unit = ()
      override def onException(ex: Exception): Unit = ex.printStackTrace()
    }

    override def onStart(): Unit = {
      redirectSystemError()
      val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
        .getInstance()
        .addListener(rateLimitedStatusListener)
        .sample("en")
      twitterStreamPromise.success(twitterStream)
    }

    override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
      twitterStream.cleanUp()
      twitterStream.shutdown()
    }
  }

  // 3. Resilient Twitter Receiver with Exponential Backoff
  // Create a fault-tolerant receiver that automatically reconnects with exponential backoff
  // when connection errors occur. Track connection statistics and implement circuit breaker pattern.
  class ResilientTwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY) {
    import scala.concurrent.ExecutionContext.Implicits.global
    import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicBoolean}
    
    private val connectionAttempts = new AtomicInteger(0)
    private val lastConnectionTime = new AtomicLong(0)
    private val isConnected = new AtomicBoolean(false)
    private val maxRetries = 10
    private val baseBackoffMs = 1000

    private def resilientStatusListener = new StatusListener {
      override def onStatus(status: Status): Unit = {
        if (!isConnected.get()) {
          isConnected.set(true)
          connectionAttempts.set(0)
          reportError("Twitter connection established", null)
        }
        store(status)
      }
      
      override def onException(ex: Exception): Unit = {
        isConnected.set(false)
        reportError("Twitter connection error", ex)
        reconnectWithBackoff()
      }
      
      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
      override def onStallWarning(warning: StallWarning): Unit = ()
    }

    private def reconnectWithBackoff(): Unit = {
      if (!isStopped() && connectionAttempts.incrementAndGet() <= maxRetries) {
        val backoffTime = Math.min(baseBackoffMs * Math.pow(2, connectionAttempts.get() - 1), 60000).toLong
        reportError(s"Reconnecting in ${backoffTime}ms (attempt ${connectionAttempts.get()})", null)
        
        new Thread(() => {
          Thread.sleep(backoffTime)
          if (!isStopped()) {
            attemptConnection()
          }
        }).start()
      } else {
        reportError("Max reconnection attempts reached. Stopping receiver.", null)
        stop("Max reconnection attempts reached")
      }
    }

    private def attemptConnection(): Unit = {
      try {
        redirectSystemError()
        val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
          .getInstance()
          .addListener(resilientStatusListener)
          .sample("en")
        lastConnectionTime.set(System.currentTimeMillis())
      } catch {
        case ex: Exception =>
          reportError("Failed to establish Twitter connection", ex)
          reconnectWithBackoff()
      }
    }

    override def onStart(): Unit = attemptConnection()
    override def onStop(): Unit = isConnected.set(false)
  }

  // run asynchronously
  override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }
}
