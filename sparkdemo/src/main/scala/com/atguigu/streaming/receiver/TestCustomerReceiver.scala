package com.atguigu.streaming.receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestCustomerReceiver {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sparkConf = new SparkConf().setAppName("TestCustomerReceiver").setMaster("local[*]")
    //创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //使用自定义数据源接收数据创建DStream
    val dStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102",9999))
    //计算wordcount并打印
    dStream.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()
    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }
}
