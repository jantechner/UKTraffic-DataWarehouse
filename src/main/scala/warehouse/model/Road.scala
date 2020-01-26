package warehouse.model

case class Road (
                  D_road_id: Long,
                  road_name: String,
                  road_category: String,
                  road_type: String,
                  start_junction_road_name: String,
                  end_junction_road_name: String
                )
