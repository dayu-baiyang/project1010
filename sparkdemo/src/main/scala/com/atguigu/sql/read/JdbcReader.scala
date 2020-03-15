package com.atguigu.sql.read

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JdbcReader {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("JdbcReader")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    //2.构建JDBC参数
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "000000")

    //3.读取MySQL数据
    spark.read.jdbc("jdbc:mysql://hadoop102:3306/gmall", "base_category1", properties).show()

    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("dbtable", " base_category2")
      .option("user", "root")
      .option("password", "000000")
      .load()
      .show()

    //4.关闭资源
    spark.stop()


  }

}
