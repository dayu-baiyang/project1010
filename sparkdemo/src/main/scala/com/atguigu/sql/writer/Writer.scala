package com.atguigu.sql.writer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Writer {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Writer")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //2.读取数据创建DF
    val df: DataFrame = spark.read.json("./data/people.json")

    //3.将DF写出
    //a.通过write方法直接指定写出的文件格式
    df.write.json("./out/json1")
    //    df.write.jdbc()
    //b.通过format指定保存的文件格式
   df.write.format("json").save("./out/json2")
//        df.write.format("jdbc").option("", "").save()


    //4.关闭资源
    spark.stop()

  }

}
