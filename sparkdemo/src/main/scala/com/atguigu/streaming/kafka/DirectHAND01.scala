package com.atguigu.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object DirectHAND01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DirectHAND01").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    val kafkaPara:Map[String,String] = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->("hadoop102:9092,hadoop103:9092,hadoop104:9092"),
      ConsumerConfig.GROUP_ID_CONFIG->"dayu01"
    )
    val fromOffsets = Map[TopicAndPartition, Long](
      TopicAndPartition("test", 0) -> 2L,
      TopicAndPartition("test", 1) -> 1L
    )

    val kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](
      ssc,
      kafkaPara,
      fromOffsets,
      (m: MessageAndMetadata[String, String]) => m)
    var offsetRanges = Array.empty[OffsetRange]

    kafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map((_.message()))
      .foreachRDD(rdd => {
      for (o <- offsetRanges) {
        println(s"${o.topic}:${o.partition}:${o.fromOffset}:${o.untilOffset}")
      }
      rdd.foreach(println)
    })

    //9.开启任务
    ssc.start()
    ssc.awaitTermination()



  }

}
