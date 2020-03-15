package com.atguigu.sql.read

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Reader")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //2.通用的加载数据方式
    //a.使用read方法直接指定文件格式->json,orc,jdbc,csv,parquet,txt
    val df1: DataFrame = spark.read.json("./data/people.json")
    //            spark.read.jdbc()
    //b.使用format指定读取的文件格式
    val df2: DataFrame = spark.read.format("json").load("./data/people.json")
    //    spark.read.format("jdbc").option("", "").load()

    //3.处理数据
    df1.show()
    df2.show()

    //4.关闭资源
    spark.stop()

  }

}
