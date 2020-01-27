package warehouse.executables

import org.apache.spark.sql.SparkSession
import warehouse.{ETL}

object RidesFactsTableEtl {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("Rides Facts Table ETL")
      .enableHiveSupport()
      .getOrCreate()

    val path = args(0)
    val etl = new ETL(path)

    etl.D_RIDES_FACTS(spark)
  }

}
