from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


spark = SparkSession.builder \
    .appName("EcommerceOrders") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///C:/Users/lenovo/OneDrive/Desktop/Iceberg/warehouse") \
    .getOrCreate()


spark.sql("""
CREATE TABLE IF NOT EXISTS spark_catalog.default.orders (
    order_id BIGINT,
    customer_id BIGINT,
    status STRING,
    total_amount DECIMAL(10,2),
    order_date DATE
)
USING iceberg
PARTITIONED BY (order_date)
""")


print("Loading Parquet data...")

orders_df = spark.read.parquet(
    "C:/Users/lenovo/OneDrive/Desktop/Iceberg/data/orders_2024.parquet"
)


orders_df = orders_df.withColumn(
    "order_date",
    to_date(col("order_date"))
)

orders_df.printSchema()
orders_df.show(5)


orders_df.writeTo("spark_catalog.default.orders").append()
print("Data loaded into Iceberg table.")


spark.sql("""
UPDATE spark_catalog.default.orders
SET status = 'cancelled'
WHERE order_id = 1
""")

print("Update executed.")


print("Time Travel Query:")
spark.sql("""
SELECT * FROM spark_catalog.default.orders
TIMESTAMP AS OF '2026-03-27 00:00:00'
""").show(5)


spark.sql("""
ALTER TABLE spark_catalog.default.orders
ADD COLUMN IF NOT EXISTS discount_code STRING
""")

print("Schema updated.")


print("Total Revenue:")
spark.sql("""
SELECT SUM(total_amount) AS total_revenue
FROM spark_catalog.default.orders
""").show()

print("Orders by Status:")
spark.sql("""
SELECT status, COUNT(*) AS count
FROM spark_catalog.default.orders
GROUP BY status
""").show()


print("Iceberg Snapshots:")
spark.sql("""
SELECT * FROM spark_catalog.default.orders.snapshots
""").show(5)


print("E-commerce Iceberg pipeline completed successfully!")

spark.stop()