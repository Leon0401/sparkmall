package com.atguigu.sparkmall.realtime


import java.util.Set

import com.atguigu.spark.common.bean.KafkaMessage
import com.atguigu.spark.common.util.SparkmallUtil
import com.atguigu.sparkmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * 需求4： 广告黑名单实时统计
  */
object Req4UserBlackListApplication {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UserBlackListApplication")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //  4.4.1 从kafka周期性获取广告点击数据
    val topic = "ads_log181111"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    //  4.4.2 将数据进行分解转换 (ts_user_ad, 1)
    val messageDStream = kafkaDStream.map(record => {
      val message: String = record.value()
      val datas: Array[String] = message.split(" ")

      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })
    /*//TODO 有问题的代码  Driver
    val jedis: Jedis = RedisUtil.getJedisClient
    val blackListSet: Set[String] = jedis.smembers("blacklist")
    messageDStream.filter(message => {

      !blackListSet.contains(message.userId)
    })*/

    val filterDStream: DStream[KafkaMessage] = messageDStream.transform(rdd => {
      val jedis: Jedis = RedisUtil.getJedisClient
      //集合都是transient修饰的，不能序列化数据，可以传对象。
      val blackListSet: Set[String] = jedis.smembers("blacklist")
      //广播变量 ：
      val broadcastSet: Broadcast[Set[String]] = streamingContext.sparkContext.broadcast(blackListSet)
      jedis.close()

      rdd.filter(message => {
        !broadcastSet.value.contains(message.userId)
      })
    })


    val mapDStream: DStream[(String, Int)] = filterDStream.map(message => {
      // 123213213 ==> 2019-04-30
      val dateString: String = SparkmallUtil.parseToStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")

      (dateString + "_" + message.userId + message.adId, 1)
    })


    //  4.4.3 将转换的结果进行聚合 (采集周期内的)(ts_user_ad, sum)
    //  4.4.4 将不同周期内的聚合数据进行累加
    //设定checkpoint路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, opt) => {
        //将当前采集周期的统计结果和checkpoint中的数据进行累加
        val sum = seq.sum + opt.getOrElse(0)
        // 将累加的结果放回到CP中
        Option(sum)
      }
    }

    //  4.4.5 累加后的结果进行判断 if sum>100
    stateDStream.foreachRDD(rdd => {
      rdd.foreach {
        case (key, sum) => {
          if (sum > 50) {
            //  4.4.6 超过阈值加入黑名单
            //将数据更新到redis中
            val jedis: Jedis = RedisUtil.getJedisClient

            // 获取用户
            val userId: String = key.split("_")(1)

            jedis.sadd("blacklist", userId)
            jedis.close()
          }
        }
      }
    })

    streamingContext.start()
  }
}
