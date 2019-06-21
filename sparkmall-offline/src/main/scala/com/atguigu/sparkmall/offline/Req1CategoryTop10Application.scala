package com.atguigu.sparkmall.offline

import java.sql.DriverManager
import java.util.UUID

import com.atguigu.spark.common.bean.UserVisitAction
import com.atguigu.spark.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 获取点击、下单和支付数量排名前 10 的品类
  * 目标数据：
  *   taskId,
  *   CategoryId,
  *   clickCount,
  *   orderCount,
  *   payCount
  */
object Req1CategoryTop10Application {
  def main(args: Array[String]): Unit = {
    //TODO 4.1 获取hive表中的数据
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
    if (SparkmallUtil.isNotEmptyString(endDate)) {
      sql.append(" and action_time <= '").append(endDate).append("' ")
    }

    sparkSession.sql("use " + SparkmallUtil.getValueFromConfig("hive.database"))
    val dataFrame = sparkSession.sql(sql.toString)
    //转换成RDD,用于统计分析
    val userVisitActionRDD: RDD[UserVisitAction] = dataFrame.as[UserVisitAction].rdd

    //println(userVisitActionRDD.count())

    //TODO 4.2 配置累加器
    //创建累加器
    val acc: CategoryCountAccumulator = new CategoryCountAccumulator

    //注册累加器
    sparkSession.sparkContext.register(acc)
    //TODO 4.3 对原始数据进行遍历，实现累加(聚合操作)
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
    //TODO 4.4 将累加的结果进行分组合并
    // （categoryid, Map( categoryId-clickclickCount, category-orderorderCount, category-paypayCount )）
    val groupMap: Map[String, mutable.HashMap[String, Long]] = result.groupBy {
      case (k, v) => {
        k.split("_")(0)
      }
    }

    //TODO 4.5 将分组合并的数据转换为一条数据
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

    //TODO 4.6 将数据排序后取前10条
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

    top10Data.foreach(println)

    //TODO 4.7 将结果保存到Mysql中
       val driver = SparkmallUtil.getValueFromConfig("jdbc.driver.class")
       val url = SparkmallUtil.getValueFromConfig("jdbc.url")
       val user = SparkmallUtil.getValueFromConfig("jdbc.user")
       val passwd = SparkmallUtil.getValueFromConfig("jdbc.password")

       Class.forName(driver)
       val connection = DriverManager.getConnection(url,user,passwd)
       val sql1 = "insert into category_top10 values (?,?,?,?,?)"
       val ps = connection.prepareStatement(sql1)
       top10Data.foreach(data=>{
         ps.setObject(1,data.taskId)
         ps.setObject(2,data.categoryId)
         ps.setObject(3,data.clickCount)
         ps.setObject(4,data.orderCount)
         ps.setObject(5,data.payCount)

         ps.executeUpdate()
       })

       ps.close()
       connection.close()
       sparkSession.stop()
  }
}

case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

/**
  * 累加器： (category-click, 100)  (category-order, 50) (category-pay, 10)
  */
class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]

  //是否为初始值
  override def isZero: Boolean = {
    map.isEmpty
  }

  //复制累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryCountAccumulator
  }

  //重置累加器
  override def reset(): Unit = {
    map.clear()
  }

  //Executor累加数据(这里是统计点击次数)
  override def add(key: String): Unit = {
    map(key) = map.getOrElse(key, 0L) + 1
  }

  //Driver合并数据
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    var map1 = map
    var map2 = other.value

    //注意： 这里一定要赋值给map，否则map一直为空
    map = map1.foldLeft(map2) {
      case (tempMap, (category, sumCount)) => {
        tempMap(category) = tempMap.getOrElse(category, 0L) + sumCount
        tempMap
      }
    }
  }

  //累加器的值
  override def value: mutable.HashMap[String, Long] = map
}