package part5twitter

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap

import scala.collection.JavaConverters._

object SentimentAnalysis {

  def createNlpProps() = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  def detectSentiment(message: String): SentimentType = {
    val pipeline = new StanfordCoreNLP(createNlpProps())
    val annotation = pipeline.process(message) // all the scores attached to this message

    // split the text into sentences and attach scores to each
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala
    val sentiments = sentences.map { sentence: CoreMap =>
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      // convert the score to a double for each sentence
      RNNCoreAnnotations.getPredictedClass(tree).toDouble
    }

    // average out all the sentiments detected in this text
    val avgSentiment =
      if (sentiments.isEmpty) -1 // Not understood
      else sentiments.sum / sentiments.length // average

    SentimentType.fromScore(avgSentiment)
  }

  /**
    * Extra Exercises
    */

  // 1. Batch Sentiment Analysis with Caching
  // The current `detectSentiment` is inefficient as it creates a new pipeline for every single message.
  // Create a method `detectSentimentBatch(messages: Seq[String])` that initializes the pipeline only once
  // and processes a batch of messages. For further optimization, implement a simple in-memory cache
  // (e.g., a Map) to store and retrieve results for recently seen sentences to avoid re-computation.
  def detectSentimentBatch(messages: Seq[String], cache: collection.mutable.Map[String, SentimentType]): Map[String, SentimentType] = {
    val pipeline = new StanfordCoreNLP(createNlpProps())
    messages.map { message =>
      message -> cache.getOrElseUpdate(message, {
        val annotation = pipeline.process(message)
        val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala
        val sentiments = sentences.map { sentence =>
          val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
          RNNCoreAnnotations.getPredictedClass(tree).toDouble
        }
        val avgSentiment = if (sentiments.isEmpty) -1 else sentiments.sum / sentiments.length
        SentimentType.fromScore(avgSentiment)
      })
    }.toMap
  }

  // 2. Aspect-Based Sentiment Analysis
  // Standard sentiment analysis provides a single score for the entire text. Enhance this to perform
  // aspect-based sentiment analysis. The method `detectAspectBasedSentiment` should take a message and a list of
  // "aspects" (keywords, e.g., "camera", "battery" for a phone review). It should return a
  // `Map[String, SentimentType]` where each key is an aspect, and the value is the sentiment of the
  // sentence(s) containing that aspect.
  def detectAspectBasedSentiment(message: String, aspects: List[String]): Map[String, SentimentType] = {
    val pipeline = new StanfordCoreNLP(createNlpProps())
    val annotation = pipeline.process(message)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala

    aspects.map { aspect =>
      val relevantSentences = sentences.filter(_.toString.toLowerCase.contains(aspect.toLowerCase))
      val sentiments = relevantSentences.map { sentence =>
        val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
        RNNCoreAnnotations.getPredictedClass(tree).toDouble
      }
      val avgSentiment = if (sentiments.isEmpty) -1 else sentiments.sum / sentiments.length
      aspect -> SentimentType.fromScore(avgSentiment)
    }.toMap
  }

  // 3. Sentiment with Confidence Score
  // The Stanford NLP library can provide a probability distribution over the sentiment classes.
  // Modify `detectSentiment` to return not just the `SentimentType`, but a tuple `(SentimentType, Double)`
  // where the second element is the confidence score (probability) of the detected sentiment class.
  // For a text with multiple sentences, the final confidence could be the average confidence of the dominant sentiment.
  def detectSentimentWithConfidence(message: String): (SentimentType, Double) = {
    val pipeline = new StanfordCoreNLP(createNlpProps())
    val annotation = pipeline.process(message)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala

    val sentimentsWithProbs = sentences.map { sentence =>
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentimentClass = RNNCoreAnnotations.getPredictedClass(tree)
      val sentimentDistribution = RNNCoreAnnotations.getPredictions(tree)
      val confidence = sentimentDistribution.get(sentimentClass)
      (sentimentClass.toDouble, confidence)
    }

    if (sentimentsWithProbs.isEmpty) {
      (SentimentType.NOT_UNDERSTOOD, 0.0)
    } else {
      val avgSentiment = sentimentsWithProbs.map(_._1).sum / sentimentsWithProbs.length
      val avgConfidence = sentimentsWithProbs.map(_._2).sum / sentimentsWithProbs.length
      (SentimentType.fromScore(avgSentiment), avgConfidence)
    }
  }
}


