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

    val res_PYC_MBNT_QUANLY_TRANGTHAI = readDelta(spark,s"$DELTA_GOLD/PYC_MBNT_QUANLY_TRANGTHAI").orderBy(col("ETL_DT"))
    val res_PYC_MBNT_ALM_USD = readDelta(spark,s"$DELTA_GOLD/PYC_MBNT_ALM_USD").orderBy(col("ETL_DT"))
    val res_PYC_MBNT_SODU_NOSTRO = readDelta(spark,s"$DELTA_GOLD/PYC_MBNT_SODU_NOSTRO").orderBy(col("ETL_DT"))

    spark.sql("DROP TABLE IF EXISTS PYC_MBNT_QUANLY_TRANGTHAI")
    res_PYC_MBNT_QUANLY_TRANGTHAI.write.saveAsTable("PYC_MBNT_QUANLY_TRANGTHAI")

    spark.sql("DROP TABLE IF EXISTS PYC_MBNT_ALM_USD")
    res_PYC_MBNT_ALM_USD.write.saveAsTable("PYC_MBNT_ALM_USD")

    spark.sql("DROP TABLE IF EXISTS PYC_MBNT_SODU_NOSTRO")
    res_PYC_MBNT_SODU_NOSTRO.write.saveAsTable("PYC_MBNT_SODU_NOSTRO")

    /*keep session alive*/
    while (true) {
      Thread.`yield`()
    }
  }
}
