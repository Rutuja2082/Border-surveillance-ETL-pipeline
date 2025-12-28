import dlt
from pyspark.sql.functions import col, when

@dlt.table(
    name="border_surveillance_silver",
    comment="Cleaned and standardized border surveillance data"
)
def border_surveillance_silver():
    df = dlt.read("border_surveillance_bronze")

    return (
        df
        .filter(col("signal_strength").isNotNull())
        .withColumn(
            "risk_level",
            when(col("signal_strength") >= 80, "HIGH")
            .when(col("signal_strength") >= 60, "MEDIUM")
            .otherwise("LOW")
        )
    )
