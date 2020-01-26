package warehouse

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import warehouse.model.{Authority, Region, RegionAuthority, Road, Time, VehicleType, Weather}

class ETL(val path: String) {
  def D_REGION_AUTHORITY(spark: SparkSession): Unit = {
    import spark.implicits._

    val all_authorities_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"$path/authorities*.csv").
      cache.
      as[Authority]

    val all_regions_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"$path/regions*.csv").
      cache.
      as[Region]

    all_authorities_DS.join(all_regions_DS, "region_ons_code").
      as[RegionAuthority].
      select("region_id", "region_name", "region_ons_code", "local_authority_ons_code", "local_authority_id", "local_authority_name").
      write.insertInto("dw.d_region_authority")
  }

  def D_ROAD(spark: SparkSession): Unit = {
    import spark.implicits._

    val roads_DF = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"$path/mainData*.csv").
      cache.
      select("road_name", "road_category", "road_type", "start_junction_road_name", "end_junction_road_name")

    roads_DF.
      dropDuplicates().
      withColumn("D_road_id", monotonically_increasing_id).
      as[Road].
      select("D_road_id", "road_name", "road_category", "road_type", "start_junction_road_name", "end_junction_road_name").
      write.insertInto("dw.d_road")
  }

  def D_TIME(spark: SparkSession): Unit = {
    import spark.implicits._

    val time_DF = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv("data/mainData*.csv").
      cache.
      select("year", "hour", "count_date")

    val setTimeStampHour: (java.sql.Timestamp, Integer) => java.sql.Timestamp = (date,hour) => {
      date.setHours(hour)
      date
    }
    val setHour = udf(setTimeStampHour)

    val timeStampToDayOfWeek: (java.sql.Timestamp) => Int = date => date.getDate
    val dayOfWeek = udf(timeStampToDayOfWeek)

    val timeStampToMonth: (java.sql.Timestamp) => Int = (date) => date.getMonth + 1
    val month = udf(timeStampToMonth)

    val timeStampToDayOfMonth: (java.sql.Timestamp) => Int = (date) => date.getDate
    val day = udf(timeStampToDayOfMonth)

    time_DF.
      dropDuplicates("count_date", "hour").
      withColumn("date", setHour($"count_date", $"hour")).
      withColumn("D_time_id", unix_timestamp(col("date"))).
      withColumn("month", month(col("date"))).
      withColumn("day", day(col("date"))).
      withColumn("dayOfWeek", dayOfWeek(col("date"))).
      as[Time].
      select("D_time_id", "year", "month", "day", "hour", "dayOfWeek").
      write.insertInto("dw.d_time")
  }

  def D_VEHICLE_TYPE(spark: SparkSession): Unit = {
    import spark.implicits._

    val vehicle_types_DS = Seq(
      VehicleType(1L, "pedal_cycles"),
      VehicleType(2L, "two_wheeled_motor_vehicles"),
      VehicleType(3L, "cars_and_taxis"),
      VehicleType(4L, "buses_and_coaches"),
      VehicleType(5L, "lgvs"),
      VehicleType(6L, "hgvs_2_rigid_axle"),
      VehicleType(7L, "hgvs_3_rigid_axle"),
      VehicleType(8L, "hgvs_4_or_more_rigid_axle"),
      VehicleType(9L, "hgvs_3_or_4_articulated_axle"),
      VehicleType(10L, "hgvs_5_articulated_axle"),
      VehicleType(11L, "hgvs_6_articulated_axle")
    ).toDS

    vehicle_types_DS.
      write.insertInto("dw.d_vehicle_type")

  }

  def D_WEATHER(spark: SparkSession): Unit = {
    import spark.implicits._

    val weather_DS = spark.read.text("data/weather.txt").
      as[String].
      map(_.split(":").last.trim)

    weather_DS.
      dropDuplicates.
      withColumnRenamed("value", "description").
      withColumn("D_weather_id", monotonically_increasing_id).
      as[Weather].
      select("D_weather_id", "description").
      write.insertInto("dw.d_weather")
  }
}
