package BigData.KafkaTwitterProducer

import java.util.HashMap
import java.util.Properties
import java.time.format.DateTimeFormatter

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
//import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.storage.StorageLevel
import BigData.KafkaTwitterProducer.SentimentDetect._;
import scala.util.Try
import scala.util.parsing.json.JSONObject
import java.util.Calendar
import org.elasticsearch.spark._
//import com.streaming.util.SentimentAnalyzer._


object SentimentAnalysis {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Sentiment Analysis").set("spark.executor.memory", "1g")
  //conf.set("es.index.auto.create", "true")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")
    System.setProperty("hadoop.home.dir", "C:/Users/VIGNESH/Desktop/Spark") 
    val Array(zk, group, topics, numThreads) = args
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topic = topics.split(",").map((_, numThreads.toInt)).toMap
    val sample = KafkaUtils.createStream(ssc, zk, group, topic).map(_._2)
    val now = Calendar.getInstance().getTime()
  
    sample.foreachRDD{(rdd, time) =>
       rdd.map(t => {
         Map(
           "Text" -> t.split(",")(0),
           "sentiment" -> detectSentiment(t.split(",")(0)).toString,
           "location" -> (t.split(",")(2),t.split(",")(1)),
           "time" -> now           
         )
       }).saveToEs("twitter1/tweetdetails");
     }
    ssc.start()
    ssc.awaitTermination()
  }
}