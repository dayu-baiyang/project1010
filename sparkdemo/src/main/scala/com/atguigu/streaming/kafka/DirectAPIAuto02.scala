package com.atguigu.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIAuto02 {

  def getSSC: StreamingContext = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DirectAPIAuto02").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.设置CK
    ssc.checkpoint("./ck3")

    //4.构建Kafka参数信息
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "yuzong"
    )

    //5.使用DirectAPI读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaPara,
      Set("test"))

    //6.计算WordCount并打印
    kafkaDStream.flatMap { case (_, value) =>
      value.split(" ")
    }.map((_, 1))
      .reduceByKey(_ + _)
      .print()

    kafkaDStream.map(_._2).print()

    //7.返回
    ssc
  }

  def main(args: Array[String]): Unit = {

    //获取或者创建StreamingContext
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck3", () => getSSC)

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
