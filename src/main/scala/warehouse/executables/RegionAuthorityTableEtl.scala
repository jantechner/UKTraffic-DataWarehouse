package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL}

object RegionAuthorityTableEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("Region Authority Table ETL")
      .enableHiveSupport()
      .getOrCreate()

    val path = args(0)
    val etl = new ETL(path)

    etl.D_REGION_AUTHORITY(spark)
  }
}
