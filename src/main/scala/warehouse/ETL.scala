package warehouse

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import warehouse.model.{Authority, MainData, Region, RegionAuthority, Road, Time, VehicleType, Weather}

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
      as[MainData].
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
      csv(s"$path/mainData*.csv").
      as[MainData].
      cache.
      select("year", "hour", "count_date")

    val setTimeStampHour: (java.sql.Timestamp, Integer) => java.sql.Timestamp = (date,hour) => {
      date.setHours(hour)
      date
    }
    val setHour = udf(setTimeStampHour)

    val timeStampToDayOfWeek: (java.sql.Timestamp) => Int = date => date.getDay
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

    val weather_DS = spark.read.text(s"$path/weather.txt").
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

  def D_RIDES_FACTS(spark: SparkSession): Unit = {
    import spark.implicits._

    def toLong(df: DataFrame, by: Seq[String]): DataFrame = {
      val (cols, types) = df.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
      require(types.distinct.size == 1, s"${types.distinct.toString}.length != 1")

      val kvs = explode(array(cols.map(c => struct(lit(c).alias("vehicle_type"), col(c).alias("vehicle_count"))): _*))
      val byExprs = by.map(col(_))

      df
        .select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq($"_kvs.vehicle_type", $"_kvs.vehicle_count"): _*)
    }

    def setTimeStampHour: (java.sql.Timestamp, Integer) => java.sql.Timestamp = (date,hour) => {
      date.setHours(hour)
      date
    }
    val setHour = udf(setTimeStampHour)

    def getTimestampFromString: String => String = line => {
      val items = line.split(" ")
      items(6) + " " + items(8)
    }
    val getTimestamp = udf(getTimestampFromString)

    def getOnsFromString: String => String = line => line.split(" ")(4)
    val getOns = udf(getOnsFromString)

    val road_DS = spark.table("dw.d_road").as[Road]
    val vehicle_type_DS = spark.table("dw.d_vehicle_type").as[VehicleType]
    val weather_types_DS = spark.table("dw.d_weather").as[Weather]

    val main_DS = spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(s"$path/mainData*.csv").
      as[MainData]

    //create weather table
    val weather_data_DS =  spark.read.text(s"$path/weather.txt").as[String]
    val weather_DS = weather_data_DS.
      withColumn("description", trim(split($"value", ":")(2)).as[String]).
      withColumn("timestamp", unix_timestamp(getTimestamp($"value"), "dd/MM/yyyy HH:mm").as[Long]).
      withColumn("ons", getOns($"value").as[String]).
      drop("value").
      join(weather_types_DS, "description").
      drop("description")

    val main_road_DF = main_DS.
      join(road_DS, Seq("road_name", "road_category", "road_type", "start_junction_road_name", "end_junction_road_name")).
      withColumn("timestamp", unix_timestamp(setHour($"count_date", $"hour")).as[Long]).
      select("direction_of_travel", "timestamp", "local_authoirty_ons_code", "D_road_id", "pedal_cycles", "two_wheeled_motor_vehicles", "cars_and_taxis", "buses_and_coaches", "lgvs", "hgvs_2_rigid_axle", "hgvs_3_rigid_axle", "hgvs_4_or_more_rigid_axle", "hgvs_3_or_4_articulated_axle", "hgvs_5_articulated_axle", "hgvs_6_articulated_axle")

    //join with weather

    val joined_with_weather_DF = main_road_DF.join(weather_DS, main_road_DF("local_authoirty_ons_code") === weather_DS("ons")).
      select($"direction_of_travel",
        main_road_DF("timestamp").alias("measure_time"),
        weather_DS("timestamp").alias("weather_time"),
        $"local_authoirty_ons_code",
        $"D_road_id",
        $"pedal_cycles", $"two_wheeled_motor_vehicles", $"cars_and_taxis", $"buses_and_coaches", $"lgvs", $"hgvs_2_rigid_axle", $"hgvs_3_rigid_axle", $"hgvs_4_or_more_rigid_axle", $"hgvs_3_or_4_articulated_axle", $"hgvs_5_articulated_axle", $"hgvs_6_articulated_axle",
        $"D_weather_id").
      withColumn("time_diff", abs(col("measure_time") - col("weather_time")).as[Long]).
      where(col("time_diff") < 86400)

    val main_road_weather_DF = joined_with_weather_DF.select($"direction_of_travel", $"measure_time", $"local_authoirty_ons_code", $"D_road_id", $"D_weather_id",
      $"pedal_cycles", $"two_wheeled_motor_vehicles", $"cars_and_taxis", $"buses_and_coaches", $"lgvs", $"hgvs_2_rigid_axle", $"hgvs_3_rigid_axle", $"hgvs_4_or_more_rigid_axle", $"hgvs_3_or_4_articulated_axle", $"hgvs_5_articulated_axle", $"hgvs_6_articulated_axle",
      $"time_diff", row_number().over(Window.partitionBy("measure_time", "D_road_id").orderBy("time_diff")).alias("rank")).where($"rank" === 1).drop("rank", "time_diff")

    val main_road_weather_exploded_DF = toLong(main_road_weather_DF, Seq("direction_of_travel", "measure_time", "local_authoirty_ons_code", "D_road_id", "D_weather_id"))

    main_road_weather_exploded_DF.
      join(vehicle_type_DS, main_road_weather_exploded_DF("vehicle_type") === vehicle_type_DS("name")).
      select($"direction_of_travel", $"local_authoirty_ons_code",  $"vehicle_count", $"measure_time".as("D_time_id"), $"D_weather_id", $"D_road_id", $"D_vehicle_type_id").
      write.insertInto("dw.f_rides")
  }

}
