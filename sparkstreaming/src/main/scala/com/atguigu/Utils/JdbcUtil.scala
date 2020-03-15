package com.atguigu.Utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSourceFactory

object JdbcUtil {

  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //判断某条数据是否存在
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var preparedStatement: PreparedStatement = null
    try {
      //预编译SQL
      preparedStatement = connection.prepareStatement(sql)
      //给占位符赋值
      for (i <- params.indices) {
        preparedStatement.setObject(i + 1, params(i))
      }
      //执行查询
      flag = preparedStatement.executeQuery().next()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  //插入一条数据
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Unit = {
    var preparedStatement: PreparedStatement = null
    try {
      //预编译SQL
      preparedStatement = connection.prepareStatement(sql)
      //给预编译SQL赋值
      for (i <- params.indices) {
        preparedStatement.setObject(i + 1, params(i))
      }
      preparedStatement.executeUpdate()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  //获取一条数据
  def getData(connection: Connection, sql: String, params: Array[Any]): Long = {
    var result = 0L
    var preparedStatement: PreparedStatement = null
    try {
      //预编译SQL
      preparedStatement = connection.prepareStatement(sql)
      //给预编译SQL赋值
      for (i <- params.indices) {
        preparedStatement.setObject(i + 1, params(i))
      }
      //执行查询获取数据
      val resultSet: ResultSet = preparedStatement.executeQuery()
      if (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  def main(args: Array[String]): Unit = {

    val connection: Connection = getConnection

    println(isExist(connection, "select * from black_list where userid=?", Array("1")))
    println(isExist(connection, "select * from black_list where userid=?", Array("2")))

    connection.close()

  }


}
