package org.example

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.codehaus.jackson.map.deser.std.StringDeserializer

object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    print("TEST")
    val brokers = "localhost:9092"
    val groupId = "GRP1"
    val sparkConf = new SparkConf().setAppName("KAFKAStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topics = List("kafkaWC")
    val params = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val messages = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics,params))
    messages.map(x => x.value())
      .flatMap(_.split(" "))
      .map(x => (x, 1))
      .reduceByKey(_+_)
      .print()
    ssc.start()
    ssc.awaitTermination()


//    val brokers = "localhost:9092"
//    val groupid = "GRP1"
//    val topics = "kafkaWC"
//
//    val conf = new SparkConf().setMaster("local[*]").setAppName("KAFKAStreaming")
//    val ssc = new StreamingContext(conf, Seconds(3))
//    var sc = ssc.sparkContext
//    sc.setLogLevel("OFF")
//
//    val topicSet = topics.split(",").toSet
//    var kafkaParams = Map[String, Object](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
//      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
//    )
//    val messages = KafkaUtils.createDirectStream[String, String](
//      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
//    )
//    val line = messages.map(_.value)
//    val words = line.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
//
//    wordCounts.print()
//
//    ssc.start()
//    ssc.awaitTermination()
  }
}
