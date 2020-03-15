package com.atguigu.Utils

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.Ads_log
import com.atguigu.Utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

object BlackListHandler {

  //时间格式化对象
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 统计单日每个用户点击每个广告的次数，并将达到100次的用户加入黑名单(MySQL的一张表)
    *
    * @param filterAdsLogDStream 根据黑名单过滤后的数据集
    */
  def saveBlackListToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {

    //1.转换数据结构 ads_log=>((dt,userid,adid),1)
    val dtUserAdToOneDStream: DStream[((String, String, String), Long)] = filterAdsLogDStream.map(ads_log => {
      //a.获取数据中的时间戳
      val timestamp: Long = ads_log.timestamp
      //b.将时间戳转换为日期
      val dt: String = sdf.format(new Date(timestamp))
      //c.组合,返回
      ((dt, ads_log.userid, ads_log.adid), 1L)
    })

    //2.统计单日用户点击各个广告的总次数
    val dtUserAdToCountDStream: DStream[((String, String, String), Long)] = dtUserAdToOneDStream.reduceByKey(_ + _)

    //3.将当前批次数据写库,并根据是否超过100次决定写入黑名单
    dtUserAdToCountDStream.foreachRDD(rdd => {
      //对每个分区写入数据,减少连接的创建
      rdd.foreachPartition(iter => {
        //获取连接
        val connection: Connection = JdbcUtil.getConnection
        //写库操作
        iter.foreach { case ((dt, userid, adid), count) =>
          //a.将当前批次数据结合MySQL中已有数据更新至MySQL
          JdbcUtil.executeUpdate(connection,
            """
              |INSERT INTO user_ad_count (dt,userid,adid,count)
              |VALUES(?,?,?,?)
              |ON DUPLICATE KEY
              |UPDATE count=count+?
            """.stripMargin,
            Array(dt, userid, adid, count, count))

          //b.获取MySQL中的数据(点击次数)
          val relust: Long = JdbcUtil.getData(
            connection,
            "SELECT COUNT FROM user_ad_count WHERE dt=? AND userid=? AND adid=?",
            Array(dt, userid, adid))

          //c.如果超过100次,则将当前用户写入黑名单
          if (relust >= 500) {
            JdbcUtil.executeUpdate(connection,
              """
                |INSERT INTO black_list (userid)
                |VALUES(?)
                |ON DUPLICATE KEY
                |UPDATE userid=?
              """.stripMargin, Array(userid, userid))
          }
        }
        //释放连接
        connection.close()
      })
    })

  }

}
