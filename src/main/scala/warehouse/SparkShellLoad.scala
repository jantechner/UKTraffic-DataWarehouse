import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait Table {
  val name: String
}

case object REGION_AUTHORITY_TABLE extends Table {
  val name = "d_region_authority"
}

case object ROAD_TABLE extends Table {
  val name = "d_road"
}

case object TIME_TABLE extends Table {
  val name = "d_time"
}

case object VEHICLE_TYPE_TABLE extends Table {
  val name = "d_vehicle_type"
}

case object WEATHER_TABLE extends Table {
  val name = "d_weather"
}

object Database {
  def all(): List[Table] = List(REGION_AUTHORITY_TABLE, ROAD_TABLE, TIME_TABLE, VEHICLE_TYPE_TABLE, WEATHER_TABLE)

  def allNames(): List[String] = all().map(_.name)
}

class SchemaCreator(spark: SparkSession) {

  def dropSchema(tables: List[String]) = {
    tables.foreach(table => spark.sql(s"DROP TABLE IF EXISTS $table"))
  }

  def createAll(): Unit = {
    dropSchema(Database.allNames())
    Database.all().foreach {
      case t@TIME_TABLE => createTimeTable(t.name)
      case t@REGION_AUTHORITY_TABLE => createRegionAuthorityTable(t.name)
      case t@ROAD_TABLE => createRoadTable(t.name)
      case t@VEHICLE_TYPE_TABLE => createVehicleTypeTable(t.name)
      case t@WEATHER_TABLE => createWeatherTable(t.name)
    }
  }

  def createTimeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
          D_time_id Long,
          year int,
          month int,
          day int,
          hour int,
          dayOfWeek int
          )
          STORED AS ORC""".stripMargin
    )
  }

  def createRegionAuthorityTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
          region_id string,
          region_name string,
          region_ons_code string,
          local_authority_ons_code string,
          local_authority_id string,
          local_authority_name string
          )
          STORED AS ORC""".stripMargin
    )
  }

  def createRoadTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
          D_road_id long,
          road_name string,
          road_category string,
          road_type string,
          start_junction_road_name string,
          end_junction_road_name string
          )
          STORED AS ORC""".stripMargin
    )
  }

  def createVehicleTypeTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
          D_vehicle_type_id long,
          name string
          )
          STORED AS ORC""".stripMargin
    )
  }

  def createWeatherTable(tableName: String): DataFrame = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $tableName(
          D_weather_id long,
          description string
          )
          STORED AS ORC""".stripMargin
    )
  }
}
