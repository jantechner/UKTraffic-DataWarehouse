package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL}

object RoadTableEtl {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("Road Table ETL")
      .enableHiveSupport()
      .getOrCreate()

    val path = args(0)
    val etl = new ETL(path)

    etl.D_ROAD(spark)
  }
}
