**Iceberg E-commerce Analytics Pipeline**
**Overview**

This project demonstrates a modern data engineering pipeline built using Apache Iceberg, Apache Spark, and Flask.

It simulates an e-commerce analytics system where raw order data (Parquet) is ingested, transformed, and stored in an Iceberg table, enabling powerful capabilities such as:

ACID transactions on data lakes
Time travel queries
Schema evolution
Real-time analytics via API endpoints

The system exposes processed data through a Flask API, which can be connected to a frontend dashboard for visualization.

**Objectives**

Build a data lakehouse pipeline using Iceberg
Ingest Parquet data into an Iceberg table
Perform updates and maintain data consistency (ACID)
Enable time travel queries
Demonstrate schema evolution
Expose analytics via REST API
Prepare for dashboard integration (charts, metrics, trends)

**Architecture**

Iceberg/
├── data/
│   └── orders_2024.parquet        # Raw dataset
│
├── warehouse/
│   └── default/
│       └── orders/               # Iceberg table storage
│           ├── data/             # Actual data files (Parquet)
│           └── metadata/         # Snapshots & schema metadata
│
├── orders.py                     # Spark pipeline (ETL + Iceberg ops)
├── app.py                        # Flask API for analytics
├── index.html                    # Frontend dashboard
├── requirements.txt
└── README.md

**Tech Stack**

Apache Spark → Data processing engine
Apache Iceberg → Table format (data lakehouse)
Flask → Backend API
Parquet → Columnar storage format
HTML / JS → Frontend dashboard

**Data Pipeline Flow**

1. Data Ingestion
Load raw Parquet file:
orders_2024.parquet
2. Iceberg Table Creation
CREATE TABLE spark_catalog.default.orders (...)
USING iceberg
PARTITIONED BY (order_date)
3. Data Load
Append data into Iceberg table
4. Data Updates (ACID)
UPDATE orders SET status = 'cancelled' WHERE order_id = 1
5. Time Travel
SELECT * FROM orders TIMESTAMP AS OF '2026-03-27'
6. Schema Evolution
ALTER TABLE orders ADD COLUMN discount_code STRING
7. Analytics
Revenue
Order counts
Status breakdown
Iceberg Concepts Demonstrated


** ACID Transactions**

**Iceberg guarantees:**

Atomicity → operations fully succeed or fail
Consistency → valid state always maintained
Isolation → concurrent queries don’t conflict
Durability → data persists reliably

**📸 Snapshots**

Every change creates a snapshot
Enables:
Time travel
Rollbacks
Audit history

**Example:**

orders.snapshots
 Metadata Layer

**Located in:**

warehouse/default/orders/metadata/

**Contains:**

Table schema
Snapshot history
File tracking
 Data Layer

**Located in:**

warehouse/default/orders/data/

**Contains:**

Partitioned Parquet files
Organized by order_date
 Warehouse

**Acts as:**

The root storage directory
Holds all Iceberg tables

**Example:**

warehouse/
Flask API
Base URL
http://127.0.0.1:5000
1. Health Check
GET /

**Response:**

Iceberg API is running 
2. Metrics Endpoint
GET /metrics

**Returns:**

{
  "total_orders": 20,
  "total_revenue": 5102.12,
  "avg_order_value": 255.106
}
3. Status Breakdown
GET /status

**Returns:**

[
  {"status": "completed", "count": 8},
  {"status": "cancelled", "count": 5}
]

**Frontend Integration**

The frontend (index.html) fetches data from Flask:

fetch("http://127.0.0.1:5000/metrics")

and renders:

Total Orders
Revenue
Average Order Value
Charts (future upgrade)



