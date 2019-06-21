package com.atguigu.sparkmall.offline


import java.sql.DriverManager
import java.util.UUID

import com.atguigu.spark.common.bean.UserVisitAction
import com.atguigu.spark.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
  * 需求2 ：Top10 热门品类中 Top10 活跃 Session 统计
  * 目标数据：
  * taskId，
  * categoryId，
  * sessionId，
  * clickCount
  */
object Req2CategorySessionTop10Application {
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

    //TODO 4.1.2 配置累加器
    //创建累加器
    val acc: CategoryCountAccumulator = new CategoryCountAccumulator

    //注册累加器
    sparkSession.sparkContext.register(acc)
    //TODO 4.1.3 对原始数据进行遍历，实现累加(聚合操作)
    //累加数据
    userVisitActionRDD.foreach(action => {
      if (action.click_category_id != -1) {
        //点击的场合
        acc.add(action.click_category_id + "_click")
      } else {
        if (action.order_category_ids != null) {
          val ids = action.order_category_ids.split(",")
          for (id <- ids) {
            acc.add(id + "_order")
          }
        } else {
          if (action.pay_category_ids != null) {
            val ids = action.pay_category_ids.split(",")
            for (id <- ids) {
              acc.add(id + "_pay")
            }
          }
        }
      }
    })
    // 获取累加器执行结果
    val result: mutable.HashMap[String, Long] = acc.value
    //TODO 4.1.4 将累加的结果进行分组合并
    // （categoryid, Map( categoryId-clickclickCount, category-orderorderCount, category-paypayCount )）
    val groupMap: Map[String, mutable.HashMap[String, Long]] = result.groupBy {
      case (k, v) => {
        k.split("_")(0)
      }
    }

    //TODO 4.1.5 将分组合并的数据转换为一条数据
    // CategoryTop10(taskeId, categoryId, clickcount, ordercount,paycount)
    //    UUID  通用唯一识别码，可以使分布式系统中的每一个元素拥有自己的唯一标识。
    val taskId: String = UUID.randomUUID().toString
    val dataList = groupMap.map {
      case (categoryid, map) => {
        CategoryTop10(
          taskId,
          categoryid,
          map.getOrElse(categoryid + "_click", 0L),
          map.getOrElse(categoryid + "_order", 0L),
          map.getOrElse(categoryid + "_pay", 0L)
        )
      }
    }.toList

    //TODO 4.1.6 将数据排序后取前10条
    val top10Data: List[CategoryTop10] = dataList.sortWith {
      case (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)

    //top10Data.foreach(println)

    //**********************************需求2 代码*************************************************
    // TODO 4.2.1 获取前10的品类数据
    // TODO 4.2.2 将当前的日志数据进行筛选过滤（品类，点击）
    //为了减少数据量，提前改变数据结构,获取前10品类id
    val categoryIds: List[String] = top10Data.map(_.categoryId)

    val filteredRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
      if (action.click_category_id != -1) {
        /* var flg = false
         for (data <- top10Data) {
           if (data.categoryId.equals(action.click_category_id)) {
             flg = true
           }
         }
         flg*/
        //注意比较时的数据类型
        categoryIds.contains("" + action.click_category_id)
      } else {
        false
      }
    })


    // TODO 4.2.3 对筛选后的数据进行聚合：
    // (categoryid_sessionidclickCount), (categoryid_sessionidclickCount)
    val categorySessionToCountRDD: RDD[(String, Long)] = filteredRDD.map {
      case action => {
        (action.click_category_id + "_" + action.session_id, 1L)
      }
    }

    // (categoryid_sessionidclickSum), (categoryid_sessionidclickSum)
    val categorySessionToSumRDD: RDD[(String, Long)] = categorySessionToCountRDD.reduceByKey(_ + _)


    // TODO 4.2.4 获取聚合后的数据然后根据品类进行分组
    //  (categoryid,(categoryid_sessionid,sum))
    val groupRDD: RDD[(String, Iterable[(String, Long)])] = categorySessionToSumRDD.groupBy {
      case (k, sum) => {
        val ks = k.split("_")
        ks(0)
      }
    }

    //TODO 4.2.5 将分组后的数据进行排序（降序），获取前10条数据
    //(categoryid,(categoryid_sessionid,sum))
    val sortRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas => {
      datas.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(10)
    })
    sortRDD.foreach(println)

    val RDD1 = sortRDD.map(t => (t._2))
    val RDD2: RDD[(String, Long)] = RDD1.flatMap(x => x)

    //********************************************************************************************
    //TODO 4.2.6 将结果保存到Mysql中
    val driver = SparkmallUtil.getValueFromConfig("jdbc.driver.class")
    val url = SparkmallUtil.getValueFromConfig("jdbc.url")
    val user = SparkmallUtil.getValueFromConfig("jdbc.user")
    val passwd = SparkmallUtil.getValueFromConfig("jdbc.password")

    Class.forName(driver)

    //性能优化


    RDD2.foreachPartition(datas => {
      val connection = DriverManager.getConnection(url, user, passwd)
      val sql1 = "insert into category_top10_session_count values (?,?,?,?)"
      val ps = connection.prepareStatement(sql1)
      //println(connection)
      for ((key, sum) <- datas) {
        val ks: Array[String] = key.toString().split("_")
        ps.setObject(1, taskId)
        ps.setObject(2, ks(0))
        ps.setObject(3, ks(1))
        ps.setObject(4, sum)

        ps.executeUpdate()

      }
    })


    //(categoryid,List(categoryid_sessionid,sum))
    //(String, List[(String, Long)])    遍历测试
    //复杂结构的遍历，不需要for嵌套for，这样做性能较低，也就是说，如果遇到复杂结构，提前改变结构再遍历。
    /*sortRDD.foreachPartition(datas=>{
      val connection = DriverManager.getConnection(url, user, passwd)
      val sql1 = "insert into category_test values (?,?,?,?)"
      val ps = connection.prepareStatement(sql1)
      for((key,v) <- datas){
        for (elem <- v) {
          val session: String = elem._1.toString.split("_")(0)
          ps.setObject(1, taskId)
          ps.setObject(2, key)
          ps.setObject(3, session)
          ps.setObject(4, elem._2)

          ps.executeUpdate()
        }
      }
    })*/


    //**************模式匹配测试************************************************************
    //模式匹配遇到复杂类型组合时，只能匹配外层类型，不能一步到位匹配内层类型，也就是说
    //case (key:String,v:List(String,Long))是可以的，但如果想直接匹配list中的元素，是万万不行的
    //case (key:String,(k1,v1):List(String,Long))是错误的。
    //    sortRDD.foreachPartition(datas => {
    //      for ((category, List(key, sum):List(String,Long)) <- datas) {
    //**************************************************************************************


    sparkSession.stop()


  }
}


/* val testRDD: RDD[(Int, List[Any])] = sparkSession.sparkContext.makeRDD(Array((1,List(1,"a")),(1,List(1,"a")),(1,List(1,"a")),(1,List(1,"a"))))
    testRDD.foreachPartition(datas=>{
      println("==================")
      for (elem <- datas) {
        println("---------------------")
      }
      println("****************")
    })
*/
