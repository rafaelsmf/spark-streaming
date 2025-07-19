package part7science

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.io.Source


object ScienceServer {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val kafkaTopic = "science"
  val kafkaBootstrapServer = "localhost:9092"

  def getProducer() = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "MyKafkaProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[Long, String](props)
  }

  def getRoute(producer: KafkaProducer[Long, String]) = {
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString("")
          )
        )
      }
    } ~
    path("api" / "report") {
      (parameter("sessionId".as[String]) & parameter("time".as[Long])) { (sessionId: String, time: Long) =>
        println(s"I've found session ID $sessionId and time = $time")

        // create a record to send to kafka
        val record = new ProducerRecord[Long, String](kafkaTopic, 0, s"$sessionId,$time")
        producer.send(record)
        producer.flush()

        complete(StatusCodes.OK) // HTTP 200
      }
    }
  }

  /**
    * Extra Exercises
    */

  // 1. Multi-Tenant Science Data Collection Server
  // Extend the server to handle multiple tenants/experiments with different Kafka topics.
  // Each tenant gets their own topic and data routing based on API keys or tenant IDs.
  def getMultiTenantRoute(producers: Map[String, KafkaProducer[Long, String]]) = {
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString("")
          )
        )
      }
    } ~
    path("api" / "v2" / "report") {
      (parameter("sessionId".as[String]) & 
       parameter("time".as[Long]) & 
       parameter("tenantId".as[String]) &
       optionalHeaderValueByName("X-API-Key")) { 
        (sessionId: String, time: Long, tenantId: String, apiKey: Option[String]) =>
        
        // Validate API key (simplified validation)
        val validApiKeys = Map(
          "tenant1" -> "key123",
          "tenant2" -> "key456",
          "tenant3" -> "key789"
        )
        
        validApiKeys.get(tenantId) match {
          case Some(expectedKey) if apiKey.contains(expectedKey) =>
            val topicName = s"science-$tenantId"
            producers.get(tenantId) match {
              case Some(producer) =>
                println(s"Recording data for tenant $tenantId: session $sessionId at time $time")
                
                // Enhanced data with metadata
                val enrichedData = Map(
                  "sessionId" -> sessionId,
                  "time" -> time.toString,
                  "tenantId" -> tenantId,
                  "timestamp" -> System.currentTimeMillis().toString,
                  "serverInstance" -> java.net.InetAddress.getLocalHost.getHostName
                )
                
                val jsonData = enrichedData.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
                val record = new ProducerRecord[Long, String](topicName, 0, jsonData)
                producer.send(record)
                producer.flush()
                
                complete(StatusCodes.OK)
                
              case None =>
                complete(StatusCodes.BadRequest, s"Unknown tenant: $tenantId")
            }
            
          case _ =>
            complete(StatusCodes.Unauthorized, "Invalid API key or tenant ID")
        }
      }
    } ~
    path("api" / "v2" / "batch") {
      post {
        entity(as[String]) { data =>
          parameter("tenantId".as[String]) { tenantId =>
            optionalHeaderValueByName("X-API-Key") { apiKey =>
              // Handle batch uploads of scientific data
              val validApiKeys = Map("tenant1" -> "key123", "tenant2" -> "key456", "tenant3" -> "key789")
              
              validApiKeys.get(tenantId) match {
                case Some(expectedKey) if apiKey.contains(expectedKey) =>
                  producers.get(tenantId) match {
                    case Some(producer) =>
                      try {
                        // Parse batch data (expecting JSON array or CSV)
                        val lines = data.split("\n").filter(_.trim.nonEmpty)
                        val topicName = s"science-$tenantId"
                        
                        lines.zipWithIndex.foreach { case (line, index) =>
                          val enrichedLine = s"""{"batchId":"${System.currentTimeMillis()}","index":$index,"data":"$line","tenantId":"$tenantId"}"""
                          val record = new ProducerRecord[Long, String](topicName, index.toLong, enrichedLine)
                          producer.send(record)
                        }
                        producer.flush()
                        
                        complete(StatusCodes.OK, s"Processed ${lines.length} records")
                      } catch {
                        case e: Exception =>
                          complete(StatusCodes.BadRequest, s"Error processing batch data: ${e.getMessage}")
                      }
                      
                    case None =>
                      complete(StatusCodes.BadRequest, s"Unknown tenant: $tenantId")
                  }
                  
                case _ =>
                  complete(StatusCodes.Unauthorized, "Invalid API key or tenant ID")
              }
            }
          }
        }
      }
    }
  }

  // 2. Rate-Limited Scientific Data Ingestion
  // Implement rate limiting to prevent overwhelming the Kafka cluster during high-volume experiments.
  def getRateLimitedRoute(producer: KafkaProducer[Long, String]) = {
    import akka.http.scaladsl.server.directives.RouteDirectives._
    import scala.collection.mutable
    import java.util.concurrent.atomic.AtomicLong
    
    // Simple in-memory rate limiter (per session)
    val rateLimiters = mutable.Map[String, (AtomicLong, Long)]() // sessionId -> (count, windowStart)
    val maxRequestsPerMinute = 100
    val windowSizeMs = 60000L // 1 minute
    
    def isRateLimited(sessionId: String): Boolean = {
      val currentTime = System.currentTimeMillis()
      
      rateLimiters.get(sessionId) match {
        case Some((counter, windowStart)) =>
          if (currentTime - windowStart > windowSizeMs) {
            // Reset window
            rateLimiters(sessionId) = (new AtomicLong(1), currentTime)
            false
          } else {
            val currentCount = counter.incrementAndGet()
            currentCount > maxRequestsPerMinute
          }
          
        case None =>
          rateLimiters(sessionId) = (new AtomicLong(1), currentTime)
          false
      }
    }
    
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString("")
          )
        )
      }
    } ~
    path("api" / "v3" / "report") {
      (parameter("sessionId".as[String]) & parameter("time".as[Long]) & parameter("experimentId".as[String].?)) { 
        (sessionId: String, time: Long, experimentId: Option[String]) =>
        
        if (isRateLimited(sessionId)) {
          complete(StatusCodes.TooManyRequests, "Rate limit exceeded. Maximum 100 requests per minute per session.")
        } else {
          // Enhanced logging and metrics
          val timestamp = System.currentTimeMillis()
          val experimentInfo = experimentId.getOrElse("default")
          
          val enhancedData = Map(
            "sessionId" -> sessionId,
            "time" -> time.toString,
            "experimentId" -> experimentInfo,
            "serverTimestamp" -> timestamp.toString,
            "processingLatency" -> (timestamp - time).toString
          )
          
          val jsonData = enhancedData.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
          
          try {
            val record = new ProducerRecord[Long, String](kafkaTopic, timestamp, jsonData)
            producer.send(record)
            producer.flush()
            
            complete(StatusCodes.OK, s"""{"status":"success","timestamp":$timestamp,"rateLimitRemaining":${maxRequestsPerMinute - rateLimiters(sessionId)._1.get()}}""")
          } catch {
            case e: Exception =>
              println(s"Error sending to Kafka: ${e.getMessage}")
              complete(StatusCodes.InternalServerError, "Failed to record data")
          }
        }
      }
    }
  }

  // 3. Real-time Experiment Monitoring Dashboard
  // Create endpoints that provide real-time statistics about ongoing experiments.
  def getMonitoringRoute(producer: KafkaProducer[Long, String]) = {
    import scala.collection.mutable
    import java.util.concurrent.atomic.AtomicLong
    
    // In-memory statistics (in production, use Redis or similar)
    val experimentStats = mutable.Map[String, AtomicLong]()
    val sessionStats = mutable.Map[String, (AtomicLong, Long)]() // (count, firstSeen)
    
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString("")
          )
        )
      }
    } ~
    path("api" / "v4" / "report") {
      (parameter("sessionId".as[String]) & 
       parameter("time".as[Long]) & 
       parameter("experimentId".as[String]) &
       parameter("eventType".as[String].?) &
       parameter("metadata".as[String].?)) { 
        (sessionId: String, time: Long, experimentId: String, eventType: Option[String], metadata: Option[String]) =>
        
        // Update statistics
        experimentStats.getOrElseUpdate(experimentId, new AtomicLong(0)).incrementAndGet()
        val currentTime = System.currentTimeMillis()
        sessionStats.get(sessionId) match {
          case Some((counter, firstSeen)) => counter.incrementAndGet()
          case None => sessionStats(sessionId) = (new AtomicLong(1), currentTime)
        }
        
        val enrichedData = Map(
          "sessionId" -> sessionId,
          "time" -> time.toString,
          "experimentId" -> experimentId,
          "eventType" -> eventType.getOrElse("measurement"),
          "metadata" -> metadata.getOrElse("{}"),
          "serverTimestamp" -> currentTime.toString
        )
        
        val jsonData = enrichedData.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
        val record = new ProducerRecord[Long, String](kafkaTopic, currentTime, jsonData)
        producer.send(record)
        producer.flush()
        
        complete(StatusCodes.OK)
      }
    } ~
    path("api" / "v4" / "stats") {
      get {
        parameter("experimentId".as[String].?) { experimentIdFilter =>
          val currentTime = System.currentTimeMillis()
          
          val statsJson = experimentIdFilter match {
            case Some(expId) =>
              val count = experimentStats.getOrElse(expId, new AtomicLong(0)).get()
              s"""{"experimentId":"$expId","totalEvents":$count,"timestamp":$currentTime}"""
              
            case None =>
              val allStats = experimentStats.map { case (expId, counter) =>
                s"""{"experimentId":"$expId","totalEvents":${counter.get()}}"""
              }.mkString("[", ",", "]")
              
              val activeSessions = sessionStats.count { case (_, (_, firstSeen)) =>
                currentTime - firstSeen < 300000 // Active in last 5 minutes
              }
              
              s"""{"experiments":$allStats,"activeSessions":$activeSessions,"timestamp":$currentTime}"""
          }
          
          complete(HttpEntity(ContentTypes.`application/json`, statsJson))
        }
      }
    } ~
    path("api" / "v4" / "health") {
      get {
        // Health check endpoint
        val healthData = Map(
          "status" -> "healthy",
          "timestamp" -> System.currentTimeMillis().toString,
          "activeExperiments" -> experimentStats.size.toString,
          "activeSessions" -> sessionStats.size.toString,
          "uptime" -> (System.currentTimeMillis() - startTime).toString
        )
        
        val healthJson = healthData.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
        complete(HttpEntity(ContentTypes.`application/json`, healthJson))
      }
    }
  }

  private val startTime = System.currentTimeMillis()

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // To run an exercise, uncomment the appropriate route
    val kafkaProducer = getProducer()
    
    // For multi-tenant setup
    // val tenantProducers = Map(
    //   "tenant1" -> getProducer(),
    //   "tenant2" -> getProducer(),
    //   "tenant3" -> getProducer()
    // )
    // val bindingFuture = Http().bindAndHandle(getMultiTenantRoute(tenantProducers), "localhost", 9988)
    
    // For rate-limited setup
    // val bindingFuture = Http().bindAndHandle(getRateLimitedRoute(kafkaProducer), "localhost", 9988)
    
    // For monitoring dashboard
    val bindingFuture = Http().bindAndHandle(getMonitoringRoute(kafkaProducer), "localhost", 9988)

    println("Science server started on http://localhost:9988")
    println("Available endpoints:")
    println("  GET  /                     - Main interface")
    println("  POST /api/v4/report        - Submit experiment data")
    println("  GET  /api/v4/stats         - Get experiment statistics")
    println("  GET  /api/v4/health        - Health check")

    // cleanup
    bindingFuture.foreach { binding =>
      binding.whenTerminated.onComplete(_ => kafkaProducer.close())
    }
  }
}
