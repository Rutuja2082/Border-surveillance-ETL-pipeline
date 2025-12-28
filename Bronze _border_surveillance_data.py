import dlt
from pyspark.sql.functions import col, when

@dlt.table(
    name="border_surveillance_bronze",
    comment="Raw border surveillance data"
)
def border_surveillance_bronze():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/main/default/v1/border_surveillance_data.csv")
    )
