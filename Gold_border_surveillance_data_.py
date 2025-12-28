import dlt
from pyspark.sql.functions import col, count

@dlt.table(
    name="border_surveillance_daily_activity",
    comment="Daily activity summary for border surveillance"
)
def daily_activity():
    df = dlt.read("border_surveillance_silver")
    return (
        df.groupBy("timestamp")
          .agg(count("*").alias("total_activity"))
    )


@dlt.table(
    name="border_surveillance_location_activity",
    comment="Location-wise activity summary"
)
def location_activity():
    df = dlt.read("border_surveillance_silver")
    return (
        df.groupBy("location")
          .agg(count("*").alias("activity_count"))
    )
