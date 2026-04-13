# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 MERGE 패턴
# MAGIC > 변경 이력을 보존하는 Slowly Changing Dimension Type 2 구현

# COMMAND ----------

from pyspark.sql.functions import col, current_date, lit, when
from delta.tables import DeltaTable

# COMMAND ----------

# 파라미터
dbutils.widgets.text("source_table", "catalog.staging.customer_updates")
dbutils.widgets.text("target_table", "catalog.silver.customers_hist")
dbutils.widgets.text("business_key", "customer_id")
dbutils.widgets.text("tracked_columns", "name,address,phone")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
business_key = dbutils.widgets.get("business_key")
tracked_cols = [c.strip() for c in dbutils.widgets.get("tracked_columns").split(",")]

# COMMAND ----------

# 소스 데이터 (변경분)
df_updates = spark.read.table(source_table)

# COMMAND ----------

# 변경 감지 조건 생성 (tracked_columns 중 하나라도 다르면 변경)
change_condition = " OR ".join([f"t.{c} != s.{c}" for c in tracked_cols])

# COMMAND ----------

if spark.catalog.tableExists(target_table):
    dt = DeltaTable.forName(spark, target_table)

    # Step 1: 기존 활성 레코드 종료 (변경된 것만)
    (dt.alias("t")
        .merge(
            df_updates.alias("s"),
            f"t.{business_key} = s.{business_key} AND t.is_current = true"
        )
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                "is_current": "false",
                "end_date": "current_date()"
            }
        )
        .execute()
    )

    # Step 2: 변경된/신규 레코드 삽입
    # 현재 활성 레코드와 비교하여 변경된 것 + 신규만 필터
    df_current = spark.read.table(target_table).filter(col("is_current") == True)

    df_new_records = (df_updates.alias("s")
        .join(df_current.alias("t"), business_key, "left_anti")  # 아예 없는 것 (신규)
    )

    df_changed_records = (df_updates.alias("s")
        .join(
            df_current.filter(col("is_current") == False).alias("closed"),
            business_key,
            "left_semi"
        )
    )

    df_to_insert = df_new_records.unionByName(df_changed_records, allowMissingColumns=True)

    if df_to_insert.count() > 0:
        (df_to_insert
            .withColumn("start_date", current_date())
            .withColumn("end_date", lit(None).cast("date"))
            .withColumn("is_current", lit(True))
            .write.mode("append")
            .saveAsTable(target_table)
        )

    print(f"✅ SCD Type 2 완료: {df_to_insert.count()} records inserted")
else:
    # 최초 생성
    (df_updates
        .withColumn("start_date", current_date())
        .withColumn("end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .write.format("delta")
        .saveAsTable(target_table)
    )
    print(f"✅ SCD Type 2 테이블 최초 생성: {target_table}")
