package com.atguigu.sparkmall.mock.util

import java.util.Date

import java.util.Random

/**
  * 生成数据用的随机工具类之日期类
  * 这是生成数据才用得上的，所以，不用放在common里。
  */
object RandomDate {
  def apply(startDate: Date, endDate: Date, step: Int): RandomDate = {
    val randomDate = new RandomDate()
    val avgStepTime = (endDate.getTime - startDate.getTime) / step
    randomDate.maxTimeStep = avgStepTime * 2
    randomDate.lastDateTime = startDate.getTime
    randomDate
  }


  class RandomDate {
    var lastDateTime = 0L
    var maxTimeStep = 0L

    def getRandomDate() = {
      //默认的随机种子是当前的时间戳
      val timeStep = new Random().nextInt(maxTimeStep.toInt)
      lastDateTime = lastDateTime + timeStep

      new Date(lastDateTime)
    }
  }

}
