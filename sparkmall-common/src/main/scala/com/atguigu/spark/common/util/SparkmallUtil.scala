package com.atguigu.spark.common.util

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Date, ResourceBundle}

import com.alibaba.fastjson.JSON
import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

/**
  * 读取配置文件工具类
  */
object SparkmallUtil {
  def main(args: Array[String]): Unit = {
    /*val user = getValueFromConfig("jdbc.user")
    println(user)*/

    println(getValueFromCondition("startAge"))
  }

  /**
    * 从config配置文件获取属性
    * @param key
    * @return
    */
  def getValueFromConfig(key: String): String = {
    getValueFromProperties("config", key)
  }


  /**
    * 从condition配置文件获取属性
    * @param key
    * @return
    */
  def getValueFromCondition(key: String): String = {
    val condition: String = getValueFromProperties("condition", "condition.params.json")
    //将字符串进行转换
    val jsonObject = JSON.parseObject(condition)
    jsonObject.getString(key)
  }


  /**
    * 读取properties配置文件
    *
    * @param fileName 配置文件名称
    * @param key      配置属性名称
    * @return
    */
  def getValueFromProperties(fileName: String, key: String): String = {
    /*1. java io流读取
    val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName)

    val properties = new java.util.Properties()
    properties.load(stream)
    properties.getProperty(key)*/


    //2. 使用第三方组件，读取配置文件  了解即可
    /* new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration]).configure(
       new Parameters().properties.setFileName(key)).getConfiguration*/

    //3. 使用国际化(i18n)组件， 只能读取properties文件
    val bunble = ResourceBundle.getBundle(fileName);
    bunble.getString(key)
  }


  /**
    * 判断字符串是否为空   true非空，false 空
    * @param s
    * @return
    */
  def isNotEmptyString(s: String): Boolean = {
    s != null && !"".equals(s.trim)
  }

  def parseToStringDateFromTs(ts:Long=System.currentTimeMillis,format:String="yyyy-MM-dd HH:mm:ss"):String={
    //小写hh表示12进制，HH是24进制。一般大的时间用大小字母表示。
    val dateFormat = new SimpleDateFormat(format)
    dateFormat.format(new Date(ts))
  }

}
