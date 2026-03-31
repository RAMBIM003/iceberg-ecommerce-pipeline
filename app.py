from flask import Flask, jsonify, render_template
from pyspark.sql import SparkSession

app = Flask(__name__)

spark = SparkSession.builder \
    .appName("IcebergAPI") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///C:/Users/lenovo/OneDrive/Desktop/Iceberg/warehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()


# 🔥 Serve UI
@app.route("/")
def home():
    return render_template("index.html")


@app.route("/metrics")
def metrics():
    df = spark.sql("""
        SELECT 
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM spark_catalog.default.orders
    """)

    row = df.collect()[0]

    return jsonify({
        "total_orders": row["total_orders"],
        "total_revenue": float(row["total_revenue"]),
        "avg_order_value": float(row["avg_order_value"])
    })


@app.route("/status")
def status():
    df = spark.sql("""
        SELECT status, COUNT(*) as count
        FROM spark_catalog.default.orders
        GROUP BY status
    """)

    rows = df.collect()

    return jsonify([
        {"status": r["status"], "count": r["count"]}
        for r in rows
    ])


if __name__ == "__main__":
    app.run(debug=True)