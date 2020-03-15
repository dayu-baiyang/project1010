package com.atguigu.streaming.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiveAPI {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf = new SparkConf().setAppName("ReceiveAPI").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //3.使用ReceiveAPI读取kafka数据创建DStream
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "bigdata1010",
      Map[String, Int]("test" -> 2))
    kafkaDStream
    //4.计算WordCount并打印
    kafkaDStream.flatMap { case (_, value) =>
      value.split(" ")
    }.map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //5.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
