# Databricks notebook source
# MAGIC %md
# MAGIC # Gold 레이어 집계 패턴
# MAGIC > Silver 데이터를 비즈니스 KPI로 집계하여 Gold 테이블을 생성합니다.

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as _sum, avg, min as _min, max as _max, current_timestamp

# COMMAND ----------

# 파라미터
dbutils.widgets.text("source_table", "catalog.silver.orders")
dbutils.widgets.text("target_table", "catalog.gold.daily_sales_summary")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

# COMMAND ----------

# 1. Silver 데이터 읽기
df_silver = spark.read.table(source_table)

# COMMAND ----------

# 2. 비즈니스 집계
df_gold = (df_silver
    .filter(col("status").isin("COMPLETED", "SHIPPED"))
    .groupBy("region", "order_date")
    .agg(
        count("order_id").alias("order_count"),
        _sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        _min("amount").alias("min_order_value"),
        _max("amount").alias("max_order_value"),
    )
    .withColumn("_aggregated_at", current_timestamp())
)

# COMMAND ----------

# 3. Gold 테이블 덮어쓰기 (전체 갱신 방식)
(df_gold.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"✅ Gold 집계 완료: {target_table} ({df_gold.count()} rows)")
