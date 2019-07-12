package cn.spark_dm

import cn.Normal.{JdbcPool, SparkSession_msg}
import cn.SparkUtils.Spark_Utils
import cn.properties_get.Mysql_JDBC
import cn.propies_load.Properties_Load
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

class Qfbap_Dws_Dm {

  def dws_Dm(sqlKey:String): Unit = {

      val spark = Spark_Utils.getEnabelHiveSparkSession(SparkSession_msg.SPARK_APPNAME,SparkSession_msg.SPARK_MASTER)
      val sql = Properties_Load.getResource(sqlKey)
      if (sql.isEmpty) {
        LoggerFactory.getLogger("sparksql").debug("sql有问题,请重新编写提交")
      }else{
        //执行sql
        val df: DataFrame = spark.sql(sql)
        //获取hive表名,需要带上数据库名
        val hive_tableName = sqlKey
        //获取mysql参数 --> name,passwd,driver   和url  以及tablename
        val prop = Mysql_JDBC.getJDBC._1
        val url = Mysql_JDBC.getJDBC._2
        val mysql_tableName = sqlKey.split("\\.")(1)
//      df.write.mode(SaveMode.Overwrite).jdbc(url,mysql_tableName,prop)

      df.write.mode(SaveMode.Append).jdbc(url,mysql_tableName,prop)


//        df.createTempView("tmp")
//      spark.sql("select * from tmp").show(10)

//      spark.sql("insert overwrite qfbap_dm.dm_user_basic partition(dt='20190711') " +
//       "select * from tmp")
//      df.write.mode(SaveMode.Append).insertInto(sqlKey)
    }
  }
}

object Qfbap_Dws_Dm {
  def main(args: Array[String]): Unit = {
    val dm = new Qfbap_Dws_Dm
    dm.dws_Dm(args(0))
  }
}
