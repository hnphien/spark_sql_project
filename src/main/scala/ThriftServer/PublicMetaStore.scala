package POC_U11_ThriftServer

import POC_U11_StreamingData.TransformFunction.readDelta
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

// noinspection SpellCheckingInspection
object PublicMetaStore {
  def main(args: Array[String]): Unit = {

    val DELTA_GOLD = args(0)
    val THRIFT_PORT = args(1)

    /*spark session*/
    val spark = SparkSession
      .builder
      .appName("Spark SQL POC U11")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("hive.server2.thrift.port", THRIFT_PORT)
      .config("spark.sql.hive.thriftServer.singleSession", true)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println(spark)

    import spark.implicits._

    /*keep session alive*/
    while (true) {
      Thread.`yield`()
    }
  }
}
