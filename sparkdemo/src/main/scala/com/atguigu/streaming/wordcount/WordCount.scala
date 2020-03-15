package com.atguigu.streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
  def main(args: Array[String]): Unit = {
//创建sparkconf
    val sparkConf = new SparkConf().setAppName("socketWordCount").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //3.接收端口数据创建DStream => hello atguigu spark scala hive
    val socketDStream = ssc.socketTextStream("hadoop102",9999)
    //4.将行数据转换为单词
    val WordDStream = socketDStream.flatMap(_.split(" "))
    //5.将单词转换为元组
    val wordToOneDStream = WordDStream.map((_,1))
      //6.按照单词统计出现的次数
      val wordToCountDStream = wordToOneDStream.reduceByKey(_+_)
      //7.打印
      wordToCountDStream.print()
      //8.开启流式任务
      ssc.start()
      ssc.awaitTermination()
  }
}
