/*
* @Package 
* @author wang shuangli
* @date 2022-06-14 23:00
* @version V1.0
* @Copyright © 2015-2022 
*/

import com.cmbc.util.PropertiesUtil
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.io.File

/*
 https://www.arangodb.com/docs/stable/drivers/spark-connector-new.html#batch-write
常规配置
 - user: db 用户，root默认
 - password: 数据库密码
 - endpoints: 协调员列表，例如c1:8529,c2:8529（必填）
 - acquireHostList：获取集群中所有已知主机的列表（true或false），false默认情况下
 - protocol：通信协议（vst或http），http默认情况下
 - contentType：驱动程序通信的内容类型（json或vpack），json默认情况下
 - timeout300000:默认情况下，驱动程序连接和请求超时，以毫秒为单位
 - ssl.enabled：ssl 安全驱动程序连接（true或false），false默认情况下
 - ssl.cert.value: Base64 编码的证书
 - ssl.cert.type: 证书类型，X.509默认
 - ssl.cert.alias: 证书别名，arangodb默认
 - ssl.algorithm：信任管理器算法，SunX509默认情况下
 - ssl.keystore.type: 密钥库类型，jks默认
 - ssl.protocol: SSLContext 协议，TLS默认

读取配置
 - database: 数据库名称，_system默认
 - table: 数据源 ArangoDB 集合名称，如果query指定则忽略。要么 要么table是query必需的。
 - query：自定义 AQL 读取查询。如果设置，table将被忽略。要么 要么table是query必需的。
 - batchSize：读取批量大小，10000默认情况下
 - sampleSize：为模式推断预取的样本大小，仅在未提供读取模式时使用，1000默认情况下
 - fillBlockCache：指定查询是否应该将读取的数据存储在 RocksDB 块缓存中（true或false），false默认情况下
 - stream: 指定查询是否应该延迟执行，true默认情况下
 - mode：允许设置在解析过程中处理损坏记录的模式：
 - PERMISSIVE: win 损坏记录的情况下，将格式错误的字符串放入由 配置的字段中 columnNameOfCorruptRecord，并将格式错误的字段设置为 null。为了保留损坏的记录，用户可以设置以columnNameOfCorruptRecord用户定义的模式命名的字符串类型字段。如果模式没有该字段，它会在解析过程中删除损坏的记录。columnNameOfCorruptRecord推断模式时，它会在输出模式中隐式添加字段
 - DROPMALFORMED：忽略整个损坏的记录
 - FAILFAST：在记录损坏的情况下抛出异常
 - columnNameOfCorruptRecord：允许重命名具有由PERMISSIVE模式创建的格式错误的字符串的新字段


写配置
 - table: 目标 ArangoDB 集合名称（必填）
 - batchSize：写入批量大小，10000默认情况下
 - byteBatchSize: 字节批量大小阈值，仅考虑contentType=json,8388608默认情况下 (8 MB)
 - table.shards：创建的集合的分片数（在Append或OverwriteSaveMode 的情况下）
 - table.type:默认情况下创建的集合的类型（document或）（如果是或保存模式）edgeAppendOverwritedocument
 - waitForSync：指定是否等到文档同步到磁盘（true或false），false默认情况下
 - confirmTruncate：默认情况下，在使用OverwriteSaveMode时确认截断表false
 - overwriteMode：在具有指定_key值的文档已经存在的情况下配置行为。仅考虑AppendSaveMode。
 - ignore（SaveMode 以外的默认值Append）：不会被写入
 - replace：它将被指定的文档值覆盖
 - update：它将使用指定的文档值进行修补（部分更新）。可以通过keepNull和mergeObjects参数进一步控制覆盖模式。keepNull也将自动设置为 true，以便空值保留在保存的文档中，而不用于删除现有的文档字段（与默认的 ArangoDB upsert 行为一样）。
 - conflict（AppendSaveMode 的默认值）：返回唯一约束违规错误，以便插入操作失败
 - mergeObjects: 如果overwriteMode设置为update，控制是否合并对象（不是数组）。
 - true（默认）：对象将被合并
 - false: 现有的文档字段将被覆盖
 - keepNull：如果overwriteMode设置为update
 - true（默认）：null值保存在文档中（默认）
 - false: nullvalues 用于删除对应的现有属性
 - retry.maxAttempts10：默认情况下，在它们是幂等的情况下重试写入请求的最大尝试次数
 - retry.minDelay：写入请求重试之间的最小延迟（毫秒），0默认情况下
 - retry.maxDelay：写入请求重试之间的最大延迟（毫秒），0默认情况下*/

object ArangoWriter {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("程序输入参数异常，应输入两个参数：1.配置文件在hdfs上的地址 2.配置文件所属用户名，请检查！")
      System.exit(0)
    }
    val configPath = args(0)
    val hdfsUser = args(1)

    PropertiesUtil.initProperties(configPath, hdfsUser)
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
    val baseOptions: Map[String, String] = Map(
      "user" -> user,
      "password" -> password,
      "endpoints" -> endpoints,
      "database" -> arangoDBName,
      "table" -> table,
      "table.type" -> tableType
    )
    val saveOptions: Map[String, String] = baseOptions ++ Map(
      "confirmTruncate" -> "true",
      "overwriteMode" -> "replace",
      "batchSize" -> batchSize,
      "table.shards" -> tableShards
    )
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val dataFrame: DataFrame = sql(hiveSQL)
    dataFrame.write
      .mode("overwrite")
      .format("com.arangodb.spark")
      .options(saveOptions)
      .save()

    spark.stop
  }

}
