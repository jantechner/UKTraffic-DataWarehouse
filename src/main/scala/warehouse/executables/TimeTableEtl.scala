package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL}

object TimeTableEtl {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("Time Table ETL")
      .enableHiveSupport()
      .getOrCreate()

    val path = args(0)
    val etl = new ETL(path)

    etl.D_TIME(spark)
  }
}
