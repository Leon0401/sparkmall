package com.atguigu.sparkmall.offline

import java.sql.DriverManager
import java.util.UUID

import com.atguigu.spark.common.bean.UserVisitAction
import com.atguigu.spark.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 需求3 ：页面单跳转化率
  *
  *   目标数据： taskId，
  *             1-2， 跳转
  *             1-2/1 跳转比率
  */
object Req3PageFlowApplication {
  def main(args: Array[String]): Unit = {
    //TODO 4.1.1 获取hive表中的数据
    //创建配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")
    //构建环境对象sparkSession  (这里需要导入core跟sql的包)
    val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
    //导入隐式转换
    import sparkSession.implicits._


    //从hive中查询数据
    var sql: StringBuilder = new StringBuilder("select * from user_visit_action where 1 = 1 ")
    val startDate = SparkmallUtil.getValueFromCondition("startDate")
    if (SparkmallUtil.isNotEmptyString(startDate)) {
      sql.append(" and action_time >= '").append(startDate).append("' ")
    }
    val endDate = SparkmallUtil.getValueFromCondition("endDate")
    if (SparkmallUtil.isNotEmptyString(startDate)) {
      sql.append(" and action_time <= '").append(endDate).append("' ")
    }

    sparkSession.sql("use " + SparkmallUtil.getValueFromConfig("hive.database"))
    val dataFrame = sparkSession.sql(sql.toString)
    //转换成RDD,用于统计分析
    val userVisitActionRDD: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd

    //println(userVisitActionRDD.count())

    //**********************************需求3 代码*************************************************

    //TODO 4.3.1 获取分母: 每一个页面点击次数的总和
    //将日志数据进行过滤，保留需要统计的页面
    val pageId: String = SparkmallUtil.getValueFromCondition("targetPageFlow")
    val pageIds: Array[String] = pageId.split(",")

    val filteredRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
      //这里的action是不管是click还是order还是pay，只要是跳转操作就行
      pageIds.contains("" + action.page_id)
    })

    //转换结构 action==>(pageId, 1L)
    val pageIdToCountRDD: RDD[(Long, Long)] = filteredRDD.map(action => {
      (action.page_id, 1L)
    })
    val pageIdToSumRDD: RDD[(Long, Long)] = pageIdToCountRDD.reduceByKey(_ + _)
    //pageIdToSumRDD.foreach(println)
    //为了方便取值，转换成map，可以遍历放入map，这里用另一种方式
    val pageIdToSumMap: Map[Long, Long] = pageIdToSumRDD.collect().toMap

    //TODO 4.3.1 获取分子
    //根据session进行分组
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action => {
      action.session_id
    })
    //将分组后的数据进行排序
    // session, List((1-2,1),(2-3,1))
    val sessionToPageCountListRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas => {
      val sortList: List[UserVisitAction] = datas.toList.sortWith {
        case (left, right) => {
          //事件顺序就是页面跳转顺序，升序
          left.action_time < right.action_time
        }
      }

      //List(1,2,3,4,5,6,7，8,9,10)  包含不需要的pageId
      val pageIdList: List[Long] = sortList.map(_.page_id)
      //(1-2,1),(2-3,1),(3-4,1),(4-5,1),(5-6,1),(6-7,1)   join , 样例类 ，zip都可以将两条数据合并成一条数据
      //页面跳转数据列表  第一次zip：是为了获取键，得到元组(键，count)
      val pageFlowList: List[(Long, Long)] = pageIdList.zip(pageIdList.tail)
      pageFlowList.map {
        case (before, after) => {
          (before + "-" + after, 1L)
        }
      }
    })

    // List( (1-2,1), (2-3,1) ) ==> (1-2,1), (2-3,1)  拿到(页面流转,count)
    val pageFlowToCountListRDD: RDD[List[(String, Long)]] = sessionToPageCountListRDD.map {
      case (session, list) => {
        list
      }
    }

    //flatMap 扁平化为 (1-2,1)
    val pageFlowToCountRDD: RDD[(String, Long)] = pageFlowToCountListRDD.flatMap(x => x)


    // 将不需要关心页面流转的数据过滤掉
    // 1，2，3，4，5，6，7
    // 1-2，2-3，3-4，4-5，5-6，6-7   第二次zip，是为了获取需求的跳转流程，方便过滤
    val zipPageIds: Array[(String, String)] = pageIds.zip(pageIds.tail)
    val zipMapPageIds: Array[String] = zipPageIds.map {
      case (before, after) => {
        before + "-" + after
      }
    }

    //将数据进行过滤，提高reduceByKey进行shuffle时的性能。
    val filterZipRDD: RDD[(String, Long)] = pageFlowToCountRDD.filter {
      case (pagflow, count) => {}
        zipMapPageIds.contains(pagflow)
    }


    // (1-2, 100)
    val pageFlowToSumRDD: RDD[(String, Long)] = filterZipRDD.reduceByKey(_ + _)

    // TODO 页面单跳点击次数 / 页面点击次数
    /*pageFlowToSumRDD.foreach {
      case (pageFlow, sum) => {
        val ks: Array[String] = pageFlow.split("-")
        println(pageFlow + "转化率 = " + (sum.toDouble / pageIdToSumMap(ks(0).toLong)))
      }
    }*/


    //********************************************************************************************
    //TODO 4.2.6 将结果保存到Mysql中
    val driver = SparkmallUtil.getValueFromConfig("jdbc.driver.class")
    val url = SparkmallUtil.getValueFromConfig("jdbc.url")
    val user = SparkmallUtil.getValueFromConfig("jdbc.user")
    val passwd = SparkmallUtil.getValueFromConfig("jdbc.password")

    Class.forName(driver)

    /*sortRDD.foreach {
      case (category, List(session, sum)) => {
        val connection = DriverManager.getConnection(url, user, passwd)
        val sql1 = "insert into category_top10 values (?,?,?,?)"
        val ps = connection.prepareStatement(sql1)
        ps.setObject(1, taskId)
        ps.setObject(2, category)
        ps.setObject(3, session)
        ps.setObject(4, sum)


        ps.executeUpdate()

        ps.close()
        connection.close()
      }
    }*/

    val taskId: String = UUID.randomUUID().toString
    pageFlowToSumRDD.foreachPartition(datas => {
          val connection = DriverManager.getConnection(url, user, passwd)
          val sql3 = "insert into category_top10_active_top10 values (?,?,?)"
          val ps = connection.prepareStatement(sql3)
          for ((pageFlow, sum) <- datas) {

            val ks: Array[String] = pageFlow.split("-")
            ps.setObject(1, taskId)
            ps.setObject(2, pageFlow)
            ps.setObject(3, sum.toDouble / pageIdToSumMap(ks(0).toLong))


            ps.executeUpdate()

          }
          ps.close()
          connection.close()
        })

    sparkSession.stop()
  }
}



