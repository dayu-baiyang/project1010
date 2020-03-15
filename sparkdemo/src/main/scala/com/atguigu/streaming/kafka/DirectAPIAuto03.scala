package com.atguigu.streaming.kafka



import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIAuto03 {
  def getSSC: StreamingContext = {
    val sparkConf = new SparkConf().setAppName("DirectAPIAuto03").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./ck")
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "dayu"
    )
    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaPara,
      Set("test")
    )
    kafkaDStream.flatMap { case (_, value) => value.split(" ")
    }.map((_, 1))
      .reduceByKey(_ + _)
      .print()
    ssc
    }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck", () => getSSC)
    ssc.start()
    ssc.awaitTermination()
  }
  }




