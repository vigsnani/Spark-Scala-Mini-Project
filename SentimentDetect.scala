package BigData.KafkaTwitterProducer

import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object SentimentDetect {
  
  val NLP = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }



  def detectSentiment(message: String): SENTIMENT_TYPE = {

    val pipeline = new StanfordCoreNLP(NLP)
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()
    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val Text = sentence.toString

      if (Text.length() > longest) {
        mainSentiment = sentiment
        longest = Text.length()
      }
      sentiments += sentiment.toDouble
      sizes += Text.length

      println("debug: " + sentiment)
      println("size: " + Text.length)
    }

    val averageSentiment:Double = {
      if(sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))
    
    if(sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }

    println("debug: main: " + mainSentiment)
    println("debug: avg: " + averageSentiment)
    println("debug: weighted: " + weightedSentiment)
    
    weightedSentiment match {
      case s if s < 1.0 => NEGATIVE
      case s if s < 2.0 => NEUTRAL
      case s if s > 2.0 => POSITIVE
    }
  }

  trait SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
}
