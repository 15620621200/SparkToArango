import com.cmbc.util.PropertiesUtil

/*
* @Package 
* @author wang shuangli
* @date 2022-06-15 0:57
* @version V1.0
* @Copyright © 2015-2022 
*/

object Test {
  def main(args: Array[String]): Unit = {
    val configPath = "E:\\Idea workplace\\SparkToArango\\src\\main\\resources\\sateudp.properties"
    PropertiesUtil.initProperties(configPath)
    val appName = PropertiesUtil.getString("app_name")
    val hiveSQL = PropertiesUtil.getString("hive_SQL")
    val user = PropertiesUtil.getString("user")
    val password = PropertiesUtil.getString("password")
    val endpoints = PropertiesUtil.getString("endpoints")
    val arangoDBName = PropertiesUtil.getString("arango_DB_name")
    val table = PropertiesUtil.getString("table")
    val tableType = PropertiesUtil.getString("table_type")
    val batchSize = PropertiesUtil.getString("batch_size")
    val tableShards = PropertiesUtil.getString("table_shards")
  }

}
