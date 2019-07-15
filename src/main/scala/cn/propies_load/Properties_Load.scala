package cn.propies_load

import java.util.Properties


object Properties_Load {

  //获取配置文件对象
  val prop = new Properties()
  try {
    //将配置文件加载进来
    val dws_dm1 = Properties_Load.getClass.getClassLoader.getResourceAsStream("dws_dm.properties")
    val dws_dm2 = Properties_Load.getClass.getClassLoader.getResourceAsStream("dm.properties")
    val mysql_jdbc = Properties_Load.getClass.getClassLoader.getResourceAsStream("mysql_jdbc.properties")

    //将加载的配置文件转载进peoperties对象中
    prop.load(dws_dm1)
    prop.load(dws_dm2)
    prop.load(mysql_jdbc)
  }catch {
    case e:Exception => print(e.getStackTrace)
  }

  /**
    * 从配置文件对象中取值
    */
  def getResource(key:String):String = {
    prop.getProperty(key)
  }
}
