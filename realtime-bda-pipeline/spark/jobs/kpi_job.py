from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window,
    countDistinct, count, sum as _sum, when
)

# ---------------------------------------
# Spark Session (MongoDB v10.x compatible)
# ---------------------------------------
spark = (
    SparkSession.builder
    .appName("CartAbandonmentKPIs")
    .config("spark.mongodb.read.connection.uri", "mongodb://mongo:27017")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

DB = "ecommerce"

# ---------------------------------------
# Helper to read Mongo collection
# ---------------------------------------
def read_mongo(collection):
    return (
        spark.read.format("mongodb")
        .option("connection.uri", "mongodb://mongo:27017")
        .option("database", DB)
        .option("collection", collection)
        .load()
    )

# ---------------------------------------
# Helper: flatten window struct for JDBC
# ---------------------------------------
def flatten_window(df):
    if "window" in df.columns:
        df = (
            df.withColumn("window_start", col("window.start"))
              .withColumn("window_end", col("window.end"))
              .drop("window")
        )
    return df

# ---------------------------------------
# Read Mongo collections
# ---------------------------------------
fact_events   = read_mongo("fact_cart_events")
dim_customers = read_mongo("dim_customers")
dim_devices   = read_mongo("dim_devices")
dim_campaigns = read_mongo("dim_campaigns")

# ---------------------------------------
# Prepare timestamps
# ---------------------------------------
events = fact_events.withColumn(
    "event_time",
    to_timestamp(col("event_time"))
)

# ---------------------------------------
# Join events with dimensions
# ---------------------------------------
events_enriched = (
    events
    .join(dim_customers, on="customer_id", how="left")
    .join(dim_devices,   on="device_id",   how="left")
    .join(dim_campaigns, on="campaign_id", how="left")
)

events_enriched.cache()

# ---------------------------------------
# KPI 1: Cart Abandonment Rate (per minute)
# ---------------------------------------
kpi_abandonment = (
    events_enriched
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(
        countDistinct("cart_id").alias("total_carts"),
        countDistinct(
            when(col("event_type") != "purchase", col("cart_id"))
        ).alias("abandoned_carts")
    )
    .withColumn(
        "abandonment_rate",
        col("abandoned_carts") / col("total_carts")
    )
)

# ---------------------------------------
# KPI 2: Conversion Rate (per minute)
# ---------------------------------------
kpi_conversion = (
    events_enriched
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(
        countDistinct("cart_id").alias("total_carts"),
        count(
            when(col("event_type") == "purchase", True)
        ).alias("purchase_events")
    )
    .withColumn(
        "conversion_rate",
        col("purchase_events") / col("total_carts")
    )
)

# ---------------------------------------
# KPI 3: Revenue per minute
# ---------------------------------------
kpi_revenue = (
    events_enriched
    .filter(col("event_type") == "purchase")
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(
        _sum("order_total").alias("revenue"),
        countDistinct("cart_id").alias("orders")
    )
)

# ---------------------------------------
# KPI 4: Total Events per minute  ✅ simple
# ---------------------------------------
kpi_total_events = (
    events_enriched
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(count("*").alias("total_events"))
)

# ---------------------------------------
# KPI 5: Unique Customers per minute ✅ simple
# ---------------------------------------
kpi_unique_customers = (
    events_enriched
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(countDistinct("customer_id").alias("unique_customers"))
)

# ---------------------------------------
# Flatten windows
# ---------------------------------------
kpi_abandonment   = flatten_window(kpi_abandonment)
kpi_conversion    = flatten_window(kpi_conversion)
kpi_revenue       = flatten_window(kpi_revenue)
kpi_total_events  = flatten_window(kpi_total_events)
kpi_unique_customers = flatten_window(kpi_unique_customers)

# ---------------------------------------
# Postgres config
# ---------------------------------------
POSTGRES_URL = "jdbc:postgresql://postgres-serving:5432/serving"
POSTGRES_PROPS = {
    "user": "serving",
    "password": "serving",
    "driver": "org.postgresql.Driver"
}

# ---------------------------------------
# Write to Postgres
# ---------------------------------------
kpi_abandonment.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, "kpi_abandonment_per_minute", properties=POSTGRES_PROPS)

kpi_conversion.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, "kpi_conversion_per_minute", properties=POSTGRES_PROPS)

kpi_revenue.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, "kpi_revenue_per_minute", properties=POSTGRES_PROPS)

kpi_total_events.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, "kpi_total_events_per_minute", properties=POSTGRES_PROPS)

kpi_unique_customers.write \
    .mode("overwrite") \
    .jdbc(POSTGRES_URL, "kpi_unique_customers_per_minute", properties=POSTGRES_PROPS)

spark.stop()
