package com.travel.sql

object VirtualStationSQL {

  lazy val h3: String =
    """
      |select
      |    order_id,
      |    city_id,
      |    starting_lng,
      |    starting_lat,
      |    getH3(starting_lat,starting_lng,12) as  h3code
      |    from haikou_view
      |""".stripMargin


  lazy val joinSql: String =
    """
      | select
      |order_id,
      |city_id,
      |starting_lng,
      |starting_lat,
      |row_number() over(partition by order_grid.h3code order by starting_lng,starting_lat asc) rn
      | from order_grid  join (
      | select h3code,count(1) as totalResult from order_grid  group by h3code having totalResult >=1
      | ) groupcount on order_grid.h3code = groupcount.h3code
      |having(rn=1)
      |""".stripMargin
}
