package cn.SparkUtils

import org.apache.spark.sql.SparkSession

object Spark_Utils {

  def getSparkSession(appName:String,master:String):SparkSession = {
    SparkSession.builder().appName(appName).master(master).getOrCreate()
  }
  def getEnabelHiveSparkSession(appName:String,master:String):SparkSession = {
    SparkSession.builder().enableHiveSupport().appName(appName).master(master).getOrCreate()
  }
}
