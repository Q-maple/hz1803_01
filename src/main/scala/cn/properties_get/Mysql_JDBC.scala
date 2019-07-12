package cn.properties_get

import java.util.Properties

import cn.propies_load.Properties_Load


object Mysql_JDBC {


  def getJDBC:(Properties,String) = {

    //获取配置对象
    val prop = new Properties()

    prop.put("user",Properties_Load.getResource("jdbc.user"))
    prop.put("password",Properties_Load.getResource("jdbc.password"))
    prop.put("driver",Properties_Load.getResource("jdbc.driver"))
    val url = Properties_Load.getResource("jdbc.url")

    (prop,url)
  }



}
