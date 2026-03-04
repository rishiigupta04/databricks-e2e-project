![](diagrams/project_architecture.png)

# 🍛 Indian Restaurant Chain Analytics - End-to-End Lakehouse on Databricks

<div align="center">

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)

**A production-grade lakehouse pipeline ingesting real-time POS orders and historical data,
transforming them through a medallion architecture, enriching reviews with LLM sentiment analysis,
and serving business insights through two interactive AIBI dashboards — all on Databricks.**

[Architecture](#-architecture) • [Tech Stack](#-tech-stack) • [Data Model](#-data-model) • [Pipeline Stages](#-pipeline-stages) • [AI Integration](#-ai-integration) • [Dashboards](#-dashboards) • [Setup](#-setup--prerequisites) • [Key Learnings](#-key-engineering-decisions--learnings)

</div>

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [Architecture](#-architecture)
3. [Tech Stack](#-tech-stack)
4. [Repository Structure](#-repository-structure)
5. [Data Sources & Synthetic Data](#-data-sources--synthetic-data)
6. [Data Model](#-data-model)
7. [Pipeline Stages](#-pipeline-stages)
   - [Bronze — Ingestion](#bronze--ingestion)
   - [Silver — Transformation](#silver--transformation)
   - [Gold — Aggregation](#gold--aggregation)
8. [AI Integration — Sentiment Analysis](#-ai-integration--sentiment-analysis)
9. [Dashboards](#-dashboards)
10. [Orchestration](#-orchestration)
11. [Unity Catalog Structure](#-unity-catalog-structure)
12. [Setup & Prerequisites](#-setup--prerequisites)
13. [Key Engineering Decisions & Learnings](#-key-engineering-decisions--learnings)
14. [Results & Metrics](#-results--metrics)
15. [What I'd Do Differently in Production](#-what-id-do-differently-in-production)

---

## 🎯 Project Overview

This project simulates a **real-world lakehouse implementation** for a large Indian restaurant chain operating **5 locations across the UAE** (2 in Abu Dhabi, 2 in Dubai, 1 in Sharjah). It was built as a comprehensive end-to-end data engineering portfolio project covering ingestion, transformation, AI enrichment, and business intelligence — all within the Databricks ecosystem.

### Business Context

| Dimension | Detail |
|-----------|--------|
| **Business** | Indian restaurant chain, UAE |
| **Locations** | 5 (Abu Dhabi ×2, Dubai ×2, Sharjah ×1) |
| **Customers** | 500 registered customers |
| **Orders** | ~8,000 historical + live streaming |
| **Reviews** | 78 customer reviews with free-text |
| **Menus** | 145 menu items across all locations |

### What This Project Demonstrates

- **Dual-source ingestion** — batch CDC from Azure SQL + real-time streaming from Azure Event Hub
- **Medallion architecture** — Bronze → Silver → Gold with Unity Catalog governance
- **AI in the pipeline** — LLM sentiment + issue classification embedded directly in SQL
- **Star schema data modeling** — fact and dimension tables for analytical workloads
- **Incremental processing** — partition-aware Gold materialized views
- **Production thinking** — data quality, cost optimization, orchestration, observability

---

## 🏗️ Architecture

### High-Level System Design

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│                                                                         │
│   ┌───────────────────────┐       ┌───────────────────────────────┐    │
│   │   Azure SQL Database  │       │     Azure Event Hub           │    │
│   │   (restaurant_ops)    │       │   (Kafka Surface — port 9093) │    │
│   │                       │       │                               │    │
│   │  • customers (500)    │       │  • Live POS orders            │    │
│   │  • restaurants (5)    │       │  • 1 order every ~3 seconds   │    │
│   │  • menu_items (145)   │       │  • JSON payload               │    │
│   │  • historical_orders  │       │                               │    │
│   │  • customer_reviews   │       └──────────────┬────────────────┘    │
│   │                       │                      │                     │
│   │  [CDC Enabled]        │               Kafka connector               │
│   └──────────┬────────────┘                      │                     │
│              │                                   │                     │
│         LakeFlow                          Spark Declarative             │
│          Connect                             Pipeline                   │
└──────────────┼───────────────────────────────────┼─────────────────────┘
               │                                   │
               ▼                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    🥉 BRONZE LAYER  (ws_dbx_project.01_bronze)          │
│                                                                         │
│  historical_orders │ customer_reviews │ orders (streaming) │ ...        │
│                                                                         │
│  Raw data, as-is from source. No transformations. Never deleted.        │
└─────────────────────────────────────────┬───────────────────────────────┘
                                          │
                               Spark Declarative Pipelines
                               + Data Quality Expectations
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    🥈 SILVER LAYER  (ws_dbx_project.02_silver)          │
│                                                                         │
│  fact_orders │ fact_order_items │ fact_reviews ★AI                      │
│  dim_customers │ dim_restaurants │ dim_menu_items                       │
│                                                                         │
│  Star schema. Typed. Quality-checked. AI-enriched reviews.              │
└─────────────────────────────────────────┬───────────────────────────────┘
                                          │
                               Materialized Views
                               (Incremental Processing)
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    🥇 GOLD LAYER  (ws_dbx_project.03_gold)              │
│                                                                         │
│  d_sales_summary │ d_restaurant_reviews │ customer_360                  │
│                                                                         │
│  Pre-aggregated. Dashboard-ready. Partitioned for incremental updates.  │
└─────────────────────────────────────────┬───────────────────────────────┘
                                          │
                              Databricks AIBI Dashboards
                                          │
                          ┌───────────────┴────────────────┐
                          ▼                                ▼
               ┌──────────────────┐            ┌──────────────────────┐
               │ Chain Performance│            │  Review Insights     │
               │   Dashboard      │            │    Dashboard         │
               └──────────────────┘            └──────────────────────┘
```

### Ingestion Architecture Detail

```
Azure SQL (CDC Enabled)                    Azure Event Hub
        │                                         │
        │  ┌──────────────────────────────────┐   │  ┌──────────────────┐
        └─►│    LakeFlow Connect              │   └─►│  Spark Streaming │
           │                                  │      │  Declarative     │
           │  ┌─────────────────────────┐     │      │  Pipeline        │
           │  │  Ingestion Gateway      │     │      │                  │
           │  │  (Classic Compute)      │     │      │  readStream      │
           │  │  - Reads CDC log        │     │      │  .format("kafka")│
           │  │  - Writes to UC Volume  │     │      │  .load()         │
           │  │  - 1 worker (policy)    │     │      │                  │
           │  └──────────┬──────────────┘     │      └────────┬─────────┘
           │             │                    │               │
           │  ┌──────────▼──────────────┐     │               │
           │  │  Ingestion Pipeline     │     │               │
           │  │  (Serverless Compute)   │     │               │
           │  │  - Reads from Volume    │     │               │
           │  │  - MERGE into Delta     │     │               │
           │  └──────────┬──────────────┘     │               │
           └─────────────┼────────────────────┘               │
                         │                                     │
                         ▼                                     ▼
                  Silver dim_*                          Bronze orders
                  (CDC upserts)                    (append + historical merge)
```

### Orchestration DAG

```
workflow_daily_master
│
├─ Task 1: pipeline_injection_eventhub
│           ↳ Consumes new orders from Event Hub → Bronze.orders
│           ↳ Trigger: manual / scheduled
│
├─ Task 2: pipeline_transformation_silver   [depends_on: Task 1]
│           ↳ Bronze → Silver star schema
│           ↳ fact_orders, fact_order_items, fact_reviews (AI)
│
└─ Task 3: pipeline_transformation_gold    [depends_on: Task 2]
            ↳ Silver → Gold KPI tables
            ↳ d_sales_summary, d_restaurant_reviews, customer_360
```

---

## 🛠️ Tech Stack

| Category | Technology | Purpose |
|----------|-----------|---------|
| **Platform** | Azure Databricks Premium | Unified compute, storage, and ML platform |
| **Storage Format** | Delta Lake | ACID transactions, time travel, MERGE/UPSERT |
| **Governance** | Unity Catalog | Three-level namespace, RBAC, lineage, audit logs |
| **Batch Ingestion** | LakeFlow Connect | Zero-code CDC ingestion from Azure SQL |
| **Stream Ingestion** | Azure Event Hub (Kafka Surface) | Real-time POS order streaming |
| **Processing** | Spark Declarative Pipelines | Declarative ETL with built-in DQ and incremental processing |
| **Languages** | PySpark · Spark SQL · Python | Transformation, pipeline, data generation |
| **AI/ML** | Mosaic AI (Meta-Llama-3) · AI_QUERY() | In-pipeline LLM sentiment analysis |
| **Orchestration** | Databricks Workflows | Multi-stage pipeline DAG with dependencies |
| **Visualization** | Databricks AIBI Dashboards | Chain performance + review insights |
| **Source DB** | Azure SQL Database | Historical orders, customers, menus |
| **Message Broker** | Azure Event Hub | Kafka-compatible streaming endpoint |
| **Infrastructure** | Azure (UAE North) | Cloud infrastructure, same region as Databricks |

---

## 📁 Repository Structure

```
databricks-restaurant-analytics/
│
├── synthetic_data/
│   ├── generators/
│   │   ├── generate_restaurants.py       # 5 UAE locations
│   │   ├── generate_customers.py         # 500 customers
│   │   ├── generate_menu_items.py        # 145 menu items across locations
│   │   ├── generate_historical_orders.py # ~8,000 orders over 6 months
│   │   ├── generate_reviews.py           # Customer reviews with ratings
│   │   └── order_generator.py            # Live order stream → Event Hub (every 3s)
│   └── data/
│       ├── restaurants.csv
│       ├── customers.csv
│       ├── menu_items.csv
│       ├── historical_orders.csv
│       └── customer_reviews.csv
│
├── ingestion/
│   ├── sql_setup/
│   │   └── enable_cdc.sql                # Enable CDC + Change Tracking on all tables
│   └── streaming/
│       └── eventhub_bronze.py            # Spark Declarative Pipeline: Event Hub → Bronze
│
├── silver/
│   ├── fact_orders.py                    # Bronze → Silver, typed, DQ checks, derived cols
│   ├── fact_order_items.py               # JSON explode → item-level rows
│   └── fact_reviews.sql                  # AI_QUERY sentiment + SQL Declarative Pipeline
│
├── gold/
│   ├── d_sales_summary.py                # Daily KPIs, partitioned by order_date
│   ├── d_restaurant_reviews.py           # Review aggregation per restaurant
│   └── customer_360.py                   # Window functions, loyalty tier, favourite items
│
├── dashboards/
│   ├── chain_performance_queries.sql     # All dataset queries for Dashboard 1
│   └── review_insights_queries.sql       # All dataset queries for Dashboard 2
│
├── setup/
│   ├── cluster_policy.json               # Minimal compute policy for LakeFlow gateway
│   └── unity_catalog_setup.sql           # Catalog, schema, and GRANT statements
│
└── README.md
```

---

## 🗃️ Data Sources & Synthetic Data

All data is synthetically generated using Python scripts in `/synthetic_data/generators/`. The data is designed to be realistic — UAE cities, Indian restaurant names, authentic menu items, and plausible order patterns.

### Historical Data (Azure SQL → Batch)

| Table | Rows | Description |
|-------|------|-------------|
| `dbo.restaurants` | 5 | One row per UAE location with address, city, opening date |
| `dbo.customers` | 500 | Registered customers with email, city, join date |
| `dbo.menu_items` | 145 | Items distributed across restaurants with categories and prices |
| `dbo.historical_orders` | ~8,000 | 6 months of order history with JSON items array |
| `dbo.customer_reviews` | 78 | Free-text reviews with star ratings (1–5) |

### Live Streaming Data (Event Hub → Real-time)

The `order_generator.py` script simulates a live POS system, pushing one order every 3 seconds to Azure Event Hub with this payload:

```json
{
  "order_id": "ORD-20241215-0042",
  "timestamp": "2024-12-15T13:47:22Z",
  "restaurant_id": "R-003",
  "items": "[{\"item_id\":\"M-021\",\"item_name\":\"Butter Chicken\",\"category\":\"Main Course\",\"quantity\":2,\"unit_price\":45.0,\"subtotal\":90.0}]",
  "total_amount": 90.0,
  "payment_method": "card",
  "order_status": "completed"
}
```

> **Note:** Items are stored as a JSON string within the order payload — a deliberate design choice to mirror a real POS system. The Silver layer explodes this into row-level `fact_order_items`.

---

## 📐 Data Model

### Star Schema (Silver Layer)

```
                              ┌─────────────────────┐
                              │   dim_customers      │
                              │─────────────────────│
                              │ PK: customer_id      │
                              │    name              │
                              │    email             │
                              │    city              │
                              │    phone             │
                              │    join_date         │
                              └──────────┬──────────┘
                                         │ 1
                                         │
┌─────────────────────┐        ┌─────────┴──────────────────────────┐        ┌──────────────────────┐
│  dim_restaurants    │        │           fact_orders               │        │   dim_menu_items     │
│─────────────────────│        │────────────────────────────────────│        │──────────────────────│
│ PK: restaurant_id   │◄───────│ PK: order_id                       │        │ PK: item_id          │
│    name             │  N:1   │ FK: restaurant_id                  │        │    item_name         │
│    city             │        │ FK: customer_id                    │        │    category          │
│    country          │        │    order_timestamp                 │        │    price             │
│    opening_date     │        │    order_date       ◄─ partitioned │        │ FK: restaurant_id    │
│    phone            │        │    order_hour                      │        └──────────────────────┘
└─────────────────────┘        │    day_of_week                     │
         │                     │    is_weekend                      │
         │ 1                   │    order_type                      │
         │                     │    item_count                      │
         │                     │    total_amount  DECIMAL(10,2)     │
         │                     │    payment_method                  │
         │                     │    order_status                    │
         │                     └───────────┬───────────┬────────────┘
         │                                 │           │
         │                              1:N│           │1:1
         │                                 │           │
         │                   ┌─────────────▼──────┐  ┌▼──────────────────────────────┐
         │                   │  fact_order_items  │  │        fact_reviews ✨AI       │
         │                   │──────────────────  │  │───────────────────────────────│
         │                   │ PK: order_id+      │  │ PK: review_id                  │
         │                   │     item_id        │  │ FK: order_id                   │
         │                   │    item_name       │  │ FK: customer_id                │
         │                   │    category        │  │ FK: restaurant_id              │
         │                   │    quantity        │  │    rating         (1-5)        │
         │                   │    unit_price      │  │    review_text    (raw)        │
         │                   │    subtotal        │  │    analysis_json  (LLM output) │
         └──────────────────►│ FK: restaurant_id  │  │    sentiment      ← AI         │
                N:1          │    order_date      │  │    issue_delivery ← AI         │
                             └────────────────────┘  │    issue_food_quality ← AI     │
                                                      │    issue_pricing ← AI          │
                                                      │    issue_portion_size ← AI     │
                                                      │    *_reason       ← AI         │
                                                      └────────────────────────────────┘
```

### Gold Layer Tables

```
┌─────────────────────────────────┐   ┌──────────────────────────────────┐   ┌──────────────────────────────┐
│       d_sales_summary           │   │     d_restaurant_reviews          │   │        customer_360          │
│  (Materialized View)            │   │     (Materialized View)           │   │    (Materialized View)       │
│─────────────────────────────────│   │──────────────────────────────────│   │──────────────────────────────│
│ PK: order_date ◄─ partitioned   │   │ PK: restaurant_id                 │   │ PK: customer_id              │
│    total_orders                 │   │    restaurant_name                │   │    name, email, city         │
│    total_revenue                │   │    city                           │   │    total_orders              │
│    avg_order_value              │   │    total_reviews                  │   │    lifetime_spend            │
│    unique_customers             │   │    avg_rating                     │   │    avg_order_value           │
│    unique_restaurants           │   │    rating_5_count                 │   │    last_order_date           │
│    dine_in_orders               │   │    rating_4_count                 │   │    loyalty_tier ←computed    │
│    takeaway_orders              │   │    rating_3_count                 │   │    total_reviews             │
│    delivery_orders              │   │    rating_2_count                 │   │    avg_rating_given          │
│                                 │   │    rating_1_count                 │   │    favourite_restaurant      │
│ Source: fact_orders             │   │    sentiment_positive_count       │   │    favourite_item            │
│ Rows: 182 (one per day)         │   │    sentiment_neutral_count        │   │    is_vip ←computed          │
│ Incremental: ✅ (date-partition) │   │    sentiment_negative_count       │   │                              │
└─────────────────────────────────┘   │    issue_delivery_count           │   │ Source: 4 Silver tables      │
                                      │    issue_food_quality_count       │   │ Rows: 500 (one per customer) │
                                      │    issue_pricing_count            │   │ Incremental: ❌ (not part'd) │
                                      │    issue_portion_size_count       │   └──────────────────────────────┘
                                      │                                   │
                                      │ Source: fact_reviews + dim_rest   │
                                      │ Rows: 5 (one per restaurant)      │
                                      └───────────────────────────────────┘
```

**Loyalty Tier Logic (customer_360):**
```
lifetime_spend >= 5000  →  Platinum
lifetime_spend >= 2000  →  Gold
lifetime_spend >= 1000  →  Silver
lifetime_spend <  1000  →  Bronze
```

---

## 🔄 Pipeline Stages

### Bronze — Ingestion

#### Source 1: Azure SQL → LakeFlow Connect (Batch CDC)

CDC (Change Data Capture) was enabled on all five source tables. SQL Server automatically logs every INSERT, UPDATE, and DELETE into change tables. LakeFlow Connect reads these change logs incrementally — only the rows that actually changed move through the pipeline.

```
Azure SQL                   LakeFlow Gateway           LakeFlow Pipeline
(CDC enabled)               (Classic Compute)          (Serverless)
     │                             │                        │
     │  cdc.dbo_customers_CT       │                        │
     │  __$operation: 4 (update)   │                        │
     │  customer_id: 42            │                        │
     │  city: "Abu Dhabi"          │                        │
     │──────────────────────────► │                        │
     │                             │ Writes to UC Volume    │
     │                             │──────────────────────► │
     │                             │                        │ MERGE INTO
     │                             │                        │ silver.dim_customers
     │                             │                        │ (1 row upserted,
     │                             │                        │  not 500k scanned)
```

**Cost optimization:** Default LakeFlow gateway auto-scales to 5 workers. A minimal cluster policy was attached via the Databricks CLI (not available in the UI for LakeFlow gateways), limiting the gateway to 1 worker — reducing ingestion compute cost by ~80%.

```bash
# Attach cluster policy to LakeFlow gateway via CLI
databricks pipelines update <GATEWAY_PIPELINE_ID> \
  --policy-id "<MINIMAL_COMPUTE_POLICY_ID>"
```

#### Source 2: Azure Event Hub → Spark Declarative Pipeline (Streaming)

The Event Hub uses the **Kafka Surface** (Standard tier feature) which exposes a Kafka-compatible endpoint at port 9093. This allows Spark's native Kafka connector to read from Event Hub without any vendor-specific SDK — the same code would work against real Kafka.

**Security model (least privilege):**

```
┌────────────────────┐     Send Policy    ┌─────────────────┐
│  order_generator   │──────────────────► │   Event Hub     │
│  (producer)        │   (write-only)     │   "orders"      │
└────────────────────┘                    │                 │
                                          │                 │
┌────────────────────┐    Listen Policy   │                 │
│  Databricks Spark  │◄───────────────────│                 │
│  (consumer)        │   (read-only)      └─────────────────┘
└────────────────────┘
```

**Historical + live merge:** After the streaming pipeline begins populating `bronze.orders` with live data, a one-time INSERT loads all 8,000 historical orders into the same table. All downstream Silver and Gold tables see a single unified orders table.

```sql
-- One-time historical backfill
INSERT INTO 01_bronze.orders
SELECT * FROM 01_bronze.historical_orders;
-- Result: 17 live + 8,000 historical = 8,017 total
```

---

### Silver — Transformation

All Silver tables are built using **Spark Declarative Pipelines**. You declare what the output table should look like — the engine handles execution order, checkpointing, incremental processing, and CDC/SCD handling.

#### fact_orders — Typed, Enriched, Quality-Checked

Key transformations applied:

| Raw (Bronze) | Cleaned (Silver) | Transformation |
|---|---|---|
| `"2024-01-15 13:00:00"` (string) | `2024-01-15 13:00:00` (Timestamp) | `to_timestamp()` |
| `timestamp` | `order_date`, `order_hour`, `day_of_week`, `is_weekend` | Date functions |
| `"171.5"` (string) | `171.50` (Decimal 10,2) | `cast()` — avoids float precision errors |
| `"[{item_id:1,...}]"` (JSON string) | `item_count: 3` (Integer) | `from_json()` + `size()` |

**Data quality expectations (7 checks — `@dp.expect_all_or_drop`):**

```
✅ valid_order_id      → order_id IS NOT NULL
✅ valid_customer_id   → customer_id IS NOT NULL
✅ valid_restaurant_id → restaurant_id IS NOT NULL
✅ valid_timestamp     → order_timestamp IS NOT NULL
✅ valid_item_count    → item_count > 0
✅ valid_total_amount  → total_amount > 0
✅ valid_payment       → payment_method IN ('cash', 'card', 'wallet')

Result: 0 violations across 8,017 rows
```

#### fact_order_items — JSON Explode

Bronze stores items as a JSON array string inside each order row. Silver explodes this into individual rows — one row per item, per order.

```
BRONZE (1 row per order):
┌──────────┬───────────────────────────────────────────────────────────┐
│ order_id │ items                                                      │
│ ORD-001  │ "[{item_id:1,item_name:'Butter Chicken',qty:2,price:45}," │
│          │  "{item_id:5,item_name:'Garlic Naan',qty:3,price:12}]"    │
└──────────┴───────────────────────────────────────────────────────────┘

               from_json() → ArrayType
               explode()   → one row per element
                     ↓

SILVER (2 rows from that one order):
┌──────────┬────────────────────┬─────────┬──────────┐
│ order_id │ item_name          │ qty     │ subtotal │
│ ORD-001  │ Butter Chicken     │ 2       │ 90.00    │
│ ORD-001  │ Garlic Naan        │ 3       │ 36.00    │
└──────────┴────────────────────┴─────────┴──────────┘

8,017 orders → ~24,000 item rows (avg 3 items per order)
```


#### Dimension Tables (via LakeFlow — Silver)

A second LakeFlow pipeline points directly at the Silver schema for master data tables. `dim_customers`, `dim_restaurants`, and `dim_menu_items` receive CDC upserts automatically. When a customer moves cities or a new menu item is added, only those changed rows flow through.

**Live CDC demo result:**
- Updated `customers.city`: Dubai → Abu Dhabi (customer_id=42)
- Inserted new menu item (item count: 145 → 146)
- Silver pipeline detected 2 upserted records — 0 full table scans

---

### Gold — Aggregation

All three Gold tables are **Materialized Views** built from Silver. The Databricks incremental processing engine uses a cost model to determine whether to do a partition-level incremental update or a full recompute on each run.

#### d_sales_summary — Daily KPI Table

```python
@dp.table(
    name="d_sales_summary",
    partition_col="order_date",          # ← enables incremental processing
    table_properties={"quality": "gold"}
)
def d_sales_summary():
    return (
        spark.read.table("02_silver.fact_orders")
            .groupBy("order_date")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("total_amount"), 2).alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct(when(col("order_type")=="dine_in",  col("order_id"))).alias("dine_in_orders"),
                countDistinct(when(col("order_type")=="takeaway", col("order_id"))).alias("takeaway_orders"),
                countDistinct(when(col("order_type")=="delivery", col("order_id"))).alias("delivery_orders"),
            )
    )
```

**Result:** 182 rows (6 months × daily). When 13 new orders arrive on a single day, only that day's partition is recomputed — not all 182 days.

#### customer_360 — Window Functions & Multi-table Join

The most complex Gold table. Joins 4 Silver tables using window functions to derive computed fields.

```
Silver Sources:
┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐   ┌─────────────────────┐
│  dim_customers  │   │   fact_orders    │   │  fact_reviews   │   │  fact_order_items   │
│  (spine table)  │   │  (order stats)   │   │  (review stats) │   │  (favourite item)   │
└────────┬────────┘   └────────┬─────────┘   └────────┬────────┘   └──────────┬──────────┘
         │                     │                       │                       │
         │         LEFT JOIN   │         LEFT JOIN     │          +window fn   │
         └──────────┬──────────┴────────────┬──────────┴──────────────────────┘
                    │                       │
                    ▼                       ▼
           ┌────────────────────────────────────────────────────────┐
           │                   customer_360                          │
           │                                                         │
           │  customer_id · name · email · city · join_date          │
           │  total_orders · lifetime_spend · avg_order_value        │
           │  last_order_date · loyalty_tier                         │
           │  total_reviews · avg_rating_given                       │
           │  favourite_restaurant · favourite_item                  │
           │  is_vip (lifetime_spend >= 5000)                        │
           └────────────────────────────────────────────────────────┘
```
```

> Verified by querying the Databricks pipeline event logs: `SELECT details.incrementalization_mode FROM event_log('pipeline_id')`

---

## ✨ AI Integration — Sentiment Analysis

### The Problem

78 customer reviews as free text. To extract business value, each review needs:
- A **sentiment label** (positive / neutral / negative)
- Four **issue flags** (delivery, food quality, pricing, portion size)
- A **reason** extracted from the text for each flagged issue

### The Solution: AI_QUERY() + Mosaic AI

`AI_QUERY()` is a native Databricks SQL function that calls a Mosaic AI Foundation Model endpoint directly from inside a SQL pipeline. No separate ML infrastructure, no model deployment — one SQL function call.

**Model used:** `databricks-meta-llama-3-3-70b-instruct` (~1 DBU per million tokens)

### Prompt Engineering for Structured Output

The key challenge is getting the LLM to return parseable JSON consistently. The prompt was engineered with four constraints:

```
1. "Return ONLY a valid JSON object"  → no surrounding text
2. Exact schema with key names        → constrains output space
3. "no preamble, no markdown"         → prevents ```json blocks
4. Inject review text at the end      → clean separation of instructions vs data
```

### The CTE Pattern

```sql
CREATE OR REFRESH STREAMING TABLE 02_silver.fact_reviews

CONSTRAINT valid_sentiment
    EXPECT (sentiment IN ('positive', 'neutral', 'negative'))
    ON VIOLATION DROP ROW   -- catches unexpected LLM output

AS
-- Step 1: Call the LLM once per review, get back a JSON string
WITH model_response AS (
    SELECT
        review_id, customer_id, restaurant_id, rating, review_text,
        AI_QUERY(
            'databricks-meta-llama-3-3-70b-instruct',
            CONCAT(
                'Return ONLY valid JSON, no preamble, no markdown:\n',
                '{"sentiment":"positive|neutral|negative",',
                '"issue_delivery":true|false,',
                '"issue_delivery_reason":"...",',
                '"issue_food_quality":true|false,',
                '"issue_food_quality_reason":"...",',
                '"issue_pricing":true|false,',
                '"issue_pricing_reason":"...",',
                '"issue_portion_size":true|false,',
                '"issue_portion_size_reason":"..."}\n',
                'Review: ', review_text
            )
        ) AS analysis_json
    FROM STREAM(01_bronze.reviews)
)
-- Step 2: Parse each JSON field into a named column
SELECT
    review_id, customer_id, restaurant_id, rating, review_text,
    GET_JSON_OBJECT(analysis_json, '$.sentiment')                  AS sentiment,
    GET_JSON_OBJECT(analysis_json, '$.issue_delivery')             AS issue_delivery,
    GET_JSON_OBJECT(analysis_json, '$.issue_delivery_reason')      AS issue_delivery_reason,
    GET_JSON_OBJECT(analysis_json, '$.issue_food_quality')         AS issue_food_quality,
    GET_JSON_OBJECT(analysis_json, '$.issue_food_quality_reason')  AS issue_food_quality_reason,
    GET_JSON_OBJECT(analysis_json, '$.issue_pricing')              AS issue_pricing,
    GET_JSON_OBJECT(analysis_json, '$.issue_pricing_reason')       AS issue_pricing_reason,
    GET_JSON_OBJECT(analysis_json, '$.issue_portion_size')         AS issue_portion_size,
    GET_JSON_OBJECT(analysis_json, '$.issue_portion_size_reason')  AS issue_portion_size_reason
FROM model_response
```

### Example Outputs

**Nuanced positive with flagged delivery issue:**
```
Input:  "Everything was well prepared and had great flavor. Minor issue with packaging."

Output: {
  "sentiment": "positive",          ← overall impression is positive
  "issue_delivery": true,           ← packaging flag correctly raised
  "issue_delivery_reason": "minor issues with packaging",
  "issue_food_quality": false,
  "issue_pricing": false,
  "issue_portion_size": false
}
```

**Specific negative:**
```
Input:  "Disappointing desserts. The sweet lassi had burnt edges."

Output: {
  "sentiment": "negative",
  "issue_food_quality": true,
  "issue_food_quality_reason": "disappointing desserts, burnt edges on sweet lassi",
  "issue_delivery": false,
  "issue_pricing": false,
  "issue_portion_size": false
}
```

**Business value unlocked:** Instead of "2-star review," the operations team now sees "12 food quality complaints this week, mostly about desserts" — actionable intelligence extracted automatically.

---

## 📊 Dashboards

Both dashboards are built in **Databricks AIBI** — no external BI tool. Data stays inside the platform; the same Unity Catalog access controls apply.

### Dashboard 1: Chain Performance

**Audience:** Restaurant chain owner / operations manager

| Widget | Source | Query Logic |
|--------|--------|-------------|
| Total Orders KPI | `d_sales_summary` | `SUM(total_orders)` across date range |
| Total Revenue KPI | `d_sales_summary` | `SUM(total_revenue)` across date range |
| Avg Order Value KPI | `fact_orders` (Silver) | `AVG(total_amount)` — **not** from Gold (avg of avgs is incorrect) |
| Active Customers KPI | `fact_orders` (Silver) | `COUNT DISTINCT(customer_id)` within date range |
| Daily Sales Line Chart | `d_sales_summary` | `total_orders` and `avg_order_value` over time |
| Top 10 Selling Items | `fact_order_items` (Silver) | `SUM(quantity)` grouped by `item_name`, LIMIT 10 |
| Peak Hour Heatmap | `fact_orders` (Silver) | `COUNT DISTINCT(order_id)` by `day_of_week` + `order_hour` |
| Revenue by Order Type | `fact_orders` (Silver) | `SUM(total_amount)` by `order_type` |
| Revenue by Category | `fact_order_items` (Silver) | `SUM(subtotal)` by `category` |

**Filtering:** Date range parameter injected into SQL `WHERE` clause at runtime (not client-side filter — data is filtered at source for performance).

### Dashboard 2: Review Insights

**Audience:** Restaurant manager (per location)

| Widget | Source | Query Logic |
|--------|--------|-------------|
| Total Reviews KPI | `d_restaurant_reviews` | `SUM(total_reviews)` |
| Avg Rating KPI | `d_restaurant_reviews` | `AVG(avg_rating)` |
| Sentiment counts (3 tiles) | `d_restaurant_reviews` | Pre-aggregated sentiment counts |
| Sentiment Trend Line | `fact_reviews` + `dim_restaurants` | `COUNT` by sentiment type + review date |
| Rating Distribution Bar | `d_restaurant_reviews` | **LATERAL VIEW STACK** to unpivot wide → long |
| Issue Categorization | `fact_reviews` + `dim_restaurants` | `COUNT WHERE issue_* = 'true'` per category |
| Recent Positive Reviews | `fact_reviews` | Filtered by `sentiment = 'positive'`, `ORDER BY review_timestamp DESC` |
| Recent Negative Reviews | `fact_reviews` | Filtered by `sentiment = 'negative'`, `ORDER BY review_timestamp DESC` |

**Filtering:** Restaurant name field filter (client-side, 5 restaurants is a small dataset).

**LATERAL VIEW STACK** — used to unpivot rating distribution columns for the bar chart:

```sql
-- Wide format (stored in Gold) → Long format (needed by chart)
SELECT restaurant_name, rating_label, rating_count
FROM 03_gold.d_restaurant_reviews
LATERAL VIEW STACK(
    5,
    '5 Stars', rating_5_count,
    '4 Stars', rating_4_count,
    '3 Stars', rating_3_count,
    '2 Stars', rating_2_count,
    '1 Star',  rating_1_count
) AS rating_label, rating_count
-- Result: 5 restaurants × 5 rating levels = 25 rows
```

---

## ⚙️ Orchestration

### Databricks Workflow

The daily pipeline is orchestrated as a three-task Databricks Workflow. Each task is a Spark Declarative Pipeline. Downstream tasks only run if upstream tasks succeed.

```
workflow_daily_master
│
├─ [Task 1] pipeline_injection_eventhub
│   Type:       Declarative Pipeline
│   Action:     Reads new orders from Event Hub → appends to Bronze.orders
│   On failure: Stop + alert
│
├─ [Task 2] pipeline_transformation_silver    [depends_on: Task 1]
│   Type:       Declarative Pipeline
│   Action:     Bronze → Silver (fact_orders, fact_order_items, fact_reviews)
│   On failure: Stop + alert
│
└─ [Task 3] pipeline_transformation_gold     [depends_on: Task 2]
    Type:       Declarative Pipeline
    Action:     Silver → Gold (d_sales_summary, d_restaurant_reviews, customer_360)
    On failure: Alert (partial Gold failure acceptable)
```

> **Note:** The LakeFlow pipelines (Bronze historical load + Silver CDC for dims) run independently of this workflow — the Bronze historical load ran once, and the Silver LakeFlow pipeline runs continuously via the gateway.

---

## 🗂️ Unity Catalog Structure

```
Metastore (UAE North — one per region)
└── Catalog: ws_dbx_project
    │
    ├── Schema: 00_landing
    │   └── (staging volumes for LakeFlow)
    │
    ├── Schema: 01_bronze
    │   ├── orders              ← streaming table (Declarative Pipeline)
    │   ├── historical_orders   ← one-time LakeFlow batch load
    │   └── reviews             ← one-time LakeFlow batch load
    │
    ├── Schema: 02_silver
    │   ├── fact_orders         ← streaming table
    │   ├── fact_order_items    ← streaming table (exploded)
    │   ├── fact_reviews        ← streaming table (AI-enriched)
    │   ├── dim_customers       ← LakeFlow CDC upserts
    │   ├── dim_restaurants     ← LakeFlow CDC upserts
    │   └── dim_menu_items      ← LakeFlow CDC upserts
    │
    └── Schema: 03_gold
        ├── d_sales_summary        ← materialized view (partitioned by order_date)
        ├── d_restaurant_reviews   ← materialized view
        └── customer_360           ← materialized view
```

---

## 🏷️ Tags

`databricks` `azure` `delta-lake` `unity-catalog` `lakeflow` `apache-spark` `pyspark` `spark-sql` `streaming` `kafka` `event-hub` `cdc` `change-data-capture` `medallion-architecture` `star-schema` `data-engineering` `etl` `elt` `mosaic-ai` `ai-query` `llm` `sentiment-analysis` `data-quality` `databricks-workflows` `aibi-dashboards` `portfolio-project`

---

<div align="center">

Built as a portfolio project demonstrating end-to-end data engineering on the Databricks Lakehouse Platform.

**Stack:** Azure Databricks · Delta Lake · Unity Catalog · LakeFlow · Spark Declarative Pipelines · Mosaic AI · AIBI

</div>
