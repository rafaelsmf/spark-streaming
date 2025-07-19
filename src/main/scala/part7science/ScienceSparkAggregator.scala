package part7science

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

object ScienceSparkAggregator {

  val spark = SparkSession.builder()
    .appName("The Science project")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  case class UserResponse(sessionId: String, clickDuration: Long)
  case class UserAvgResponse(sessionId: String, avgDuration: Double)

  def readUserResponses(): Dataset[UserResponse] = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "science")
    .load()
    .select("value")
    .as[String]
    .map { line =>
      val tokens = line.split(",")
      val sessionId = tokens(0)
      val time = tokens(1).toLong

      UserResponse(sessionId, time)
    }

  /*
   Aggregate the ROLLING average response time over the past 3 clicks


   uurt("abc", [100, 200, 300, 400, 500, 600], Empty) => Iterator(200, 300, 400, 500)

   100 -> state becomes [100]
   200 -> state becomes [100, 200]
   300 -> state becomes [100, 200, 300] -> first average 200
   400 -> state becomes [200, 300, 400] -> next average 300
   500 -> state becomes [300, 400, 500] -> next average 400
   600 -> state becomes [400, 500, 600] -> next average 500

   Iterator will contain 200, 300, 400, 500

   Real time:
    61159462-0bb4-42b1-aa4b-ac242b3444a0,1186
    61159462-0bb4-42b1-aa4b-ac242b3444a0,615
    61159462-0bb4-42b1-aa4b-ac242b3444a0,1497
    61159462-0bb4-42b1-aa4b-ac242b3444a0,542
    61159462-0bb4-42b1-aa4b-ac242b3444a0,720

    window 1 = [1186, 615, 1497] = 1099.3
    window 2 = [615, 1497, 542] = 884.6
    window 3 = [1497, 542, 720] = 919.6

    next batch

    61159462-0bb4-42b1-aa4b-ac242b3444a0,768
    61159462-0bb4-42b1-aa4b-ac242b3444a0,583
    61159462-0bb4-42b1-aa4b-ac242b3444a0,485
    61159462-0bb4-42b1-aa4b-ac242b3444a0,469
    61159462-0bb4-42b1-aa4b-ac242b3444a0,566
    61159462-0bb4-42b1-aa4b-ac242b3444a0,486

    window 4 = [542, 720, 768] = 676.6
  */

  def updateUserResponseTime
    (n: Int)
    (sessionId: String, group: Iterator[UserResponse], state: GroupState[List[UserResponse]])
  : Iterator[UserAvgResponse] = {
    group.flatMap { record =>
      val lastWindow =
        if (state.exists) state.get
        else List()

      val windowLength = lastWindow.length
      val newWindow =
        if (windowLength >= n) lastWindow.tail :+ record
        else lastWindow :+ record

      // for Spark to give us access to the state in the next batch
      state.update(newWindow)

      if (newWindow.length >= n) {
        val newAverage = newWindow.map(_.clickDuration).sum * 1.0 / n
        Iterator(UserAvgResponse(sessionId, newAverage))
      } else {
        Iterator()
      }
    }
  }

  def getAverageResponseTime(n: Int) = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updateUserResponseTime(n))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def logUserResponses() = {
    readUserResponses().writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getAverageResponseTime(3)
  }

  /**
    * Extra Exercises
    */

  // 1. Multi-Window Size Analysis
  // Implement a system that simultaneously tracks multiple window sizes (3, 5, 10 clicks)
  // and detects when response times are consistently improving or degrading across all windows.
  case class MultiWindowState(window3: List[UserResponse], window5: List[UserResponse], window10: List[UserResponse])
  case class MultiWindowAvg(sessionId: String, avg3: Double, avg5: Double, avg10: Double, trend: String, timestamp: Long)

  def updateMultiWindowAnalysis
    (sessionId: String, group: Iterator[UserResponse], state: GroupState[MultiWindowState])
  : Iterator[MultiWindowAvg] = {
    group.flatMap { record =>
      val currentState = if (state.exists) state.get else MultiWindowState(List(), List(), List())
      
      // Update all windows
      val newWindow3 = if (currentState.window3.length >= 3) currentState.window3.tail :+ record else currentState.window3 :+ record
      val newWindow5 = if (currentState.window5.length >= 5) currentState.window5.tail :+ record else currentState.window5 :+ record
      val newWindow10 = if (currentState.window10.length >= 10) currentState.window10.tail :+ record else currentState.window10 :+ record
      
      val newState = MultiWindowState(newWindow3, newWindow5, newWindow10)
      state.update(newState)
      
      // Calculate averages only if all windows have minimum data
      if (newWindow3.length >= 3 && newWindow5.length >= 5 && newWindow10.length >= 10) {
        val avg3 = newWindow3.map(_.clickDuration).sum / 3.0
        val avg5 = newWindow5.map(_.clickDuration).sum / 5.0
        val avg10 = newWindow10.map(_.clickDuration).sum / 10.0
        
        // Determine trend based on window comparisons
        val trend = if (avg3 < avg5 && avg5 < avg10) "improving"
                   else if (avg3 > avg5 && avg5 > avg10) "degrading"
                   else if (math.abs(avg3 - avg5) < 50 && math.abs(avg5 - avg10) < 50) "stable"
                   else "fluctuating"
        
        Iterator(MultiWindowAvg(sessionId, avg3, avg5, avg10, trend, System.currentTimeMillis()))
      } else {
        Iterator()
      }
    }
  }

  def getMultiWindowAnalysis() = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updateMultiWindowAnalysis)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // 2. Adaptive Performance Threshold Detection
  // Create a system that learns each user's baseline performance and detects anomalies
  // when performance deviates significantly from their personal baseline.
  case class PerformanceProfile(
    sessionId: String,
    totalClicks: Long,
    runningSum: Double,
    runningSquareSum: Double,
    recentResponses: List[Double],
    lastAnomalyTime: Long
  )
  
  case class PerformanceAnomaly(
    sessionId: String,
    currentResponse: Long,
    expectedRange: (Double, Double),
    severity: String,
    confidence: Double,
    timestamp: Long
  )

  def updatePerformanceProfile
    (sessionId: String, group: Iterator[UserResponse], state: GroupState[PerformanceProfile])
  : Iterator[PerformanceAnomaly] = {
    group.flatMap { record =>
      val profile = if (state.exists) state.get 
                   else PerformanceProfile(sessionId, 0, 0.0, 0.0, List(), 0L)
      
      val currentResponse = record.clickDuration.toDouble
      val newTotalClicks = profile.totalClicks + 1
      val newRunningSum = profile.runningSum + currentResponse
      val newRunningSquareSum = profile.runningSquareSum + (currentResponse * currentResponse)
      
      // Keep last 20 responses for recent trend analysis
      val newRecentResponses = (profile.recentResponses :+ currentResponse).takeRight(20)
      
      val newProfile = PerformanceProfile(
        sessionId, newTotalClicks, newRunningSum, newRunningSquareSum, 
        newRecentResponses, profile.lastAnomalyTime
      )
      
      // Detect anomalies only after sufficient data (at least 10 clicks)
      if (newTotalClicks >= 10) {
        val mean = newRunningSum / newTotalClicks
        val variance = (newRunningSquareSum / newTotalClicks) - (mean * mean)
        val stdDev = math.sqrt(variance)
        
        // Calculate z-score for current response
        val zScore = if (stdDev > 0) math.abs(currentResponse - mean) / stdDev else 0.0
        
        // Define anomaly thresholds
        val currentTime = System.currentTimeMillis()
        val timeSinceLastAnomaly = currentTime - profile.lastAnomalyTime
        
        // Adaptive thresholds based on recent performance
        val recentMean = if (newRecentResponses.nonEmpty) newRecentResponses.sum / newRecentResponses.length else mean
        val isRecentDrift = math.abs(recentMean - mean) > stdDev * 0.5
        
        val anomalyThreshold = if (isRecentDrift) 2.0 else 2.5 // Lower threshold if recent drift detected
        
        if (zScore > anomalyThreshold && timeSinceLastAnomaly > 5000) { // At least 5 seconds between anomalies
          val severity = if (zScore > 3.5) "high" else if (zScore > 2.8) "medium" else "low"
          val confidence = math.min(0.99, (zScore - anomalyThreshold) / (4.0 - anomalyThreshold))
          val expectedRange = (mean - 2 * stdDev, mean + 2 * stdDev)
          
          // Update last anomaly time
          state.update(newProfile.copy(lastAnomalyTime = currentTime))
          
          Iterator(PerformanceAnomaly(sessionId, record.clickDuration, expectedRange, severity, confidence, currentTime))
        } else {
          state.update(newProfile)
          Iterator()
        }
      } else {
        state.update(newProfile)
        Iterator()
      }
    }
  }

  def getPerformanceAnomalyDetection() = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updatePerformanceProfile)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // 3. Session Quality Scoring with Fatigue Detection
  // Implement a comprehensive scoring system that tracks user engagement, fatigue patterns,
  // and session quality over time with early warning alerts.
  case class SessionQuality(
    sessionId: String,
    totalClicks: Long,
    consecutiveSlowClicks: Int,
    bestStreak: Int,
    currentStreak: Int,
    performanceHistory: List[Double],
    fatigueScore: Double,
    engagementScore: Double,
    lastClickTime: Long,
    sessionStartTime: Long
  )
  
  case class QualityReport(
    sessionId: String,
    overallScore: Double,
    fatigueLevel: String,
    engagementLevel: String,
    recommendation: String,
    sessionDuration: Long,
    averageResponseTime: Double,
    timestamp: Long
  )

  def updateSessionQuality
    (sessionId: String, group: Iterator[UserResponse], state: GroupState[SessionQuality])
  : Iterator[QualityReport] = {
    group.flatMap { record =>
      val currentTime = System.currentTimeMillis()
      val quality = if (state.exists) state.get 
                   else SessionQuality(sessionId, 0, 0, 0, 0, List(), 0.0, 1.0, currentTime, currentTime)
      
      val responseTime = record.clickDuration.toDouble
      val timeSinceLastClick = currentTime - quality.lastClickTime
      
      // Update performance metrics
      val newPerformanceHistory = (quality.performanceHistory :+ responseTime).takeRight(50)
      val newTotalClicks = quality.totalClicks + 1
      
      // Calculate streaks and patterns
      val isSlowClick = responseTime > 1000 // Response time > 1 second considered slow
      val newConsecutiveSlowClicks = if (isSlowClick) quality.consecutiveSlowClicks + 1 else 0
      
      val isFastClick = responseTime < 500 // Response time < 0.5 seconds considered fast
      val newCurrentStreak = if (isFastClick) quality.currentStreak + 1 else 0
      val newBestStreak = math.max(quality.bestStreak, newCurrentStreak)
      
      // Calculate fatigue score (higher is more fatigued)
      val fatigueFromSlowClicks = math.min(1.0, newConsecutiveSlowClicks / 10.0)
      val fatigueFromTimingGaps = if (timeSinceLastClick > 10000) 0.3 else 0.0 // Long gaps indicate fatigue
      val fatigueFromTrend = if (newPerformanceHistory.length >= 10) {
        val recent5 = newPerformanceHistory.takeRight(5).sum / 5.0
        val previous5 = newPerformanceHistory.dropRight(5).takeRight(5).sum / 5.0
        if (recent5 > previous5 * 1.3) 0.4 else 0.0 // Recent performance much worse
      } else 0.0
      
      val newFatigueScore = math.min(1.0, fatigueFromSlowClicks + fatigueFromTimingGaps + fatigueFromTrend)
      
      // Calculate engagement score (higher is better)
      val consistencyBonus = if (newPerformanceHistory.length >= 5) {
        val variance = {
          val mean = newPerformanceHistory.sum / newPerformanceHistory.length
          val squaredDiffs = newPerformanceHistory.map(x => (x - mean) * (x - mean))
          squaredDiffs.sum / squaredDiffs.length
        }
        val stdDev = math.sqrt(variance)
        val consistencyScore = math.max(0.0, 1.0 - (stdDev / 1000.0)) // Lower variance = higher consistency
        consistencyScore * 0.4
      } else 0.0
      
      val streakBonus = math.min(0.4, newBestStreak / 20.0)
      val baseEngagement = 1.0 - newFatigueScore
      val newEngagementScore = math.max(0.0, math.min(1.0, baseEngagement + consistencyBonus + streakBonus))
      
      val newQuality = SessionQuality(
        sessionId, newTotalClicks, newConsecutiveSlowClicks, newBestStreak, newCurrentStreak,
        newPerformanceHistory, newFatigueScore, newEngagementScore, currentTime, quality.sessionStartTime
      )
      
      state.update(newQuality)
      
      // Generate reports every 10 clicks or when significant changes occur
      if (newTotalClicks % 10 == 0 || newConsecutiveSlowClicks >= 5 || newFatigueScore > 0.7) {
        val sessionDuration = currentTime - quality.sessionStartTime
        val averageResponseTime = if (newPerformanceHistory.nonEmpty) newPerformanceHistory.sum / newPerformanceHistory.length else 0.0
        val overallScore = (newEngagementScore * 0.7) + ((1.0 - newFatigueScore) * 0.3)
        
        val fatigueLevel = if (newFatigueScore > 0.7) "high" else if (newFatigueScore > 0.4) "moderate" else "low"
        val engagementLevel = if (newEngagementScore > 0.8) "high" else if (newEngagementScore > 0.5) "moderate" else "low"
        
        val recommendation = (fatigueLevel, engagementLevel) match {
          case ("high", _) => "Consider taking a break - fatigue detected"
          case ("moderate", "low") => "Performance declining - short break recommended"
          case (_, "high") => "Excellent performance - keep up the great work!"
          case _ => "Steady performance - maintain current pace"
        }
        
        Iterator(QualityReport(sessionId, overallScore, fatigueLevel, engagementLevel, recommendation, 
                              sessionDuration, averageResponseTime, currentTime))
      } else {
        Iterator()
      }
    }
  }

  def getSessionQualityAnalysis() = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout())(updateSessionQuality)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

}
