package part5twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterProject {

  val spark = SparkSession.builder()
    .appName("The Twitter Project")
    .master("local[*]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readTwitter(): Unit = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
    val tweets: DStream[String] = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText

      s"User $username ($followers followers) says: $text"
    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Exercises
    * 1. Compute average tweet length in the past 5 seconds, every 5 seconds. Use a window function.
    * 2. Compute the most popular hashtags (hashtags most used) during a 1 minute window, update every 10 seconds.
    */

  def getAverageTweetLength(): DStream[Double] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)
    tweets
      .map(status => status.getText)
      .map(text => text.length)
      .map(len => (len, 1))
      .reduceByWindow((tuple1, tuple2) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2), Seconds(5), Seconds(5))
      .map { megaTuple  =>
        val tweetLengthSum = megaTuple._1
        val tweetCount = megaTuple._2

        tweetLengthSum * 1.0 / tweetCount
      }
  }

  def computeMostPopularHashtags(): DStream[(String, Int)] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)

    ssc.checkpoint("checkpoints")

    tweets
      .flatMap(_.getText.split(" "))
      .filter(_.startsWith("#"))
      .map(hashtag => (hashtag, 1))
      .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(60), Seconds(10))
      .transform(rdd => rdd.sortBy(tuple => - tuple._2))
  }

  def readTwitterWithSentiments(): Unit = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
    val tweets: DStream[String] = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText
      val sentiment = SentimentAnalysis.detectSentiment(text) // a single "marker"

      s"User $username ($followers followers) says $sentiment: $text"
    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Extra Exercises
    */

  // 1. Real-time Trending Topic Detection with Time Decay
  // Implement a trending hashtag detector that considers not just frequency but also recency.
  // Use a time-decaying algorithm where recent hashtags get higher weights.
  // For each hashtag, maintain a weighted score that decreases over time and increases with new mentions.
  def detectTrendingTopicsWithDecay(): DStream[(String, Double)] = {
    ssc.checkpoint("checkpoints")
    val tweets = ssc.receiverStream(new TwitterReceiver)
    
    val hashtags = tweets
      .flatMap(_.getText.split(" "))
      .filter(_.startsWith("#"))
      .map(hashtag => (hashtag, System.currentTimeMillis()))

    // Use updateStateByKey to maintain decaying scores
    val updateFunction = (values: Seq[Long], state: Option[(Double, Long)]) => {
      val currentTime = System.currentTimeMillis()
      val decayFactor = 0.95 // Score multiplied by this factor per second
      val boostValue = 1.0   // Score increment per new mention
      
      val (currentScore, lastUpdate) = state.getOrElse((0.0, currentTime))
      val timeDiff = (currentTime - lastUpdate) / 1000.0 // seconds
      val decayedScore = currentScore * Math.pow(decayFactor, timeDiff)
      val newScore = decayedScore + (values.length * boostValue)
      
      Some((newScore, currentTime))
    }

    hashtags
      .updateStateByKey(updateFunction)
      .map { case (hashtag, (score, _)) => (hashtag, score) }
      .filter(_._2 > 0.1) // Filter out very low scores
      .transform(rdd => rdd.sortBy(-_._2).zipWithIndex().filter(_._2 < 10).map(_._1)) // Top 10
  }

  // 2. User Influence Score Calculation
  // Calculate a real-time influence score for Twitter users based on their follower count,
  // retweet frequency, and mention frequency. The score should be updated in real-time as new tweets arrive.
  case class UserInfluence(userId: Long, followerCount: Int, retweetCount: Int, mentionCount: Int, lastSeen: Long)
  
  def calculateUserInfluenceScores(): DStream[(String, Double)] = {
    ssc.checkpoint("checkpoints")
    val tweets = ssc.receiverStream(new TwitterReceiver)
    
    val userMetrics = tweets.map { status =>
      val user = status.getUser
      val isRetweet = if (status.isRetweet) 1 else 0
      val mentionCount = status.getUserMentionEntities.length
      
      (user.getId, (user.getName, user.getFollowersCount, isRetweet, mentionCount, System.currentTimeMillis()))
    }

    val updateInfluence = (values: Seq[(String, Int, Int, Int, Long)], state: Option[UserInfluence]) => {
      if (values.nonEmpty) {
        val (name, followers, retweets, mentions, timestamp) = values.last
        val currentInfluence = state.getOrElse(UserInfluence(values.head._1.toLong, 0, 0, 0, 0))
        
        val newInfluence = UserInfluence(
          currentInfluence.userId,
          followers,
          currentInfluence.retweetCount + retweets,
          currentInfluence.mentionCount + mentions,
          timestamp
        )
        Some(newInfluence)
      } else {
        state
      }
    }

    userMetrics
      .updateStateByKey(updateInfluence)
      .map { case (userId, influence) =>
        val followerWeight = Math.log(influence.followerCount + 1) * 0.3
        val retweetWeight = influence.retweetCount * 0.5
        val mentionWeight = influence.mentionCount * 0.2
        val score = followerWeight + retweetWeight + mentionWeight
        
        (userId.toString, score)
      }
      .filter(_._2 > 1.0)
      .transform(rdd => rdd.sortBy(-_._2))
  }

  // 3. Real-time Tweet Clustering by Content Similarity
  // Group tweets into clusters based on content similarity using a simple TF-IDF approach.
  // For each batch, compute TF-IDF vectors for tweets and group similar tweets together.
  def clusterTweetsByContent(): DStream[(Int, List[String])] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)
    
    tweets.window(Seconds(30), Seconds(30)) // Process every 30 seconds
      .map(_.getText.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", ""))
      .transform { rdd =>
        if (!rdd.isEmpty()) {
          val tweetTexts = rdd.collect()
          val clusters = scala.collection.mutable.Map[Int, List[String]]()
          var clusterId = 0
          
          // Simple clustering by keyword similarity
          tweetTexts.foreach { tweet =>
            val words = tweet.split("\\s+").toSet
            var assigned = false
            
            for ((id, clusterTweets) <- clusters if !assigned) {
              if (clusterTweets.nonEmpty) {
                val firstTweetWords = clusterTweets.head.split("\\s+").toSet
                val similarity = words.intersect(firstTweetWords).size.toDouble / words.union(firstTweetWords).size
                
                if (similarity > 0.3) { // Similarity threshold
                  clusters(id) = tweet :: clusterTweets
                  assigned = true
                }
              }
            }
            
            if (!assigned) {
              clusters(clusterId) = List(tweet)
              clusterId += 1
            }
          }
          
          spark.sparkContext.parallelize(clusters.toSeq)
        } else {
          spark.sparkContext.emptyRDD[(Int, List[String])]
        }
      }
  }

  // 4. Anomaly Detection in Tweet Patterns
  // Detect unusual spikes in tweet volume, sentiment changes, or hashtag usage patterns.
  // Use statistical methods to identify when current metrics deviate significantly from historical patterns.
  def detectTweetAnomalies(): DStream[String] = {
    ssc.checkpoint("checkpoints")
    val tweets = ssc.receiverStream(new TwitterReceiver)
    
    // Track tweet volume in 1-minute windows
    val tweetVolume = tweets.window(Seconds(60), Seconds(30))
      .count()
      .map(count => ("volume", count.toDouble))
    
    // Maintain historical statistics using updateStateByKey
    val updateStats = (values: Seq[Double], state: Option[(Double, Double, Int)]) => {
      val (currentMean, currentVariance, sampleCount) = state.getOrElse((0.0, 0.0, 0))
      
      if (values.nonEmpty) {
        val newValue = values.head
        val newCount = sampleCount + 1
        val newMean = (currentMean * sampleCount + newValue) / newCount
        val newVariance = if (newCount > 1) {
          ((currentVariance * (sampleCount - 1)) + (newValue - newMean) * (newValue - currentMean)) / (newCount - 1)
        } else 0.0
        
        Some((newMean, newVariance, newCount))
      } else {
        state
      }
    }
    
    tweetVolume.updateStateByKey(updateStats)
      .filter { case (_, (mean, variance, count)) => count > 5 } // Need sufficient history
      .transform { rdd =>
        rdd.filter { case (metricName, (mean, variance, count)) =>
          val stdDev = Math.sqrt(variance)
          val zScore = if (stdDev > 0) Math.abs(mean - mean) / stdDev else 0.0 // This would compare current vs historical
          zScore > 2.0 // Anomaly threshold
        }.map { case (metricName, (mean, variance, count)) =>
          s"ANOMALY DETECTED in $metricName: mean=$mean, variance=$variance, samples=$count"
        }
      }
  }

  def main(args: Array[String]): Unit = {
    // To run an exercise, uncomment it.
    // Note: You'll need valid Twitter API credentials configured in twitter4j.properties
    // detectTrendingTopicsWithDecay().print()
    // calculateUserInfluenceScores().print()
    // clusterTweetsByContent().print()
    // detectTweetAnomalies().print()
    readTwitterWithSentiments()
  }
}
