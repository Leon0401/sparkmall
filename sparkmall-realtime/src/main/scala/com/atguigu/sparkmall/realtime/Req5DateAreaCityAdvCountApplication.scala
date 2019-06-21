package com.atguigu.sparkmall.realtime

import com.atguigu.spark.common.bean.KafkaMessage
import com.atguigu.spark.common.util.SparkmallUtil
import com.atguigu.sparkmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req5DateAreaCityAdvCountApplication {
  def main(args: Array[String]): Unit = {

    // 创建Spark流式数据处理环境e
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req5UserBlackListApplication")

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    // TODO 4.1 从kafka中周期性获取广告点击数据
    val topic = "ads_log181111"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    // TODO 4.2 将数据进行分解和转换：（ts_user_ad, 1）
    val messageDStream: DStream[KafkaMessage] = kafkaDStream.map(record => {

      val message: String = record.value()
      val datas: Array[String] = message.split(" ")

      KafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    // 将采集周期中的数据进行聚合
    val mapDStream: DStream[(String, Int)] = messageDStream.map(message => {
      val dateString: String = SparkmallUtil.parseToStringDateFromTs(message.ts.toLong, "yyyy-MM-dd")
      (dateString + "_" + message.area + "_" + message.city + "_" + message.adId, 1)
    })

    streamingContext.sparkContext.setCheckpointDir("cp")

    val stateDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, opt) => {
        val sum = seq.sum + opt.getOrElse(0)
        Option(sum)
      }
    }

    // 将聚合后的数据更新到redis中
    stateDStream.foreachRDD(rdd=>{

      rdd.foreachPartition(datas=>{
        val jedis: Jedis = RedisUtil.getJedisClient
        for ((field, sum) <- datas) {
          val key = "date:area:city:ads"
          jedis.hset(key, field, "" + sum)

        }
        jedis.close()

      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()


  }
}
