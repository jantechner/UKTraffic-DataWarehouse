package warehouse.model

case class RegionAuthority (
                             region_id: String,
                             region_name: String,
                             region_ons_code: String,
                             local_authority_ons_code: String,
                             local_authority_id: String,
                             local_authority_name: String
                           )
