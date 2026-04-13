# Databricks notebook source
# MAGIC %md
# MAGIC # Silver 레이어 정제/변환 패턴
# MAGIC > Bronze 데이터를 정제하고, MERGE(Upsert)로 Silver 테이블에 반영합니다.

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp, trim, upper, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# 파라미터
dbutils.widgets.text("source_table", "catalog.bronze.raw_orders")
dbutils.widgets.text("target_table", "catalog.silver.orders")
dbutils.widgets.text("merge_keys", "order_id")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
merge_keys = [k.strip() for k in dbutils.widgets.get("merge_keys").split(",")]

# COMMAND ----------

# 1. Bronze 데이터 읽기
df_raw = spark.read.table(source_table)
print(f"Bronze 레코드 수: {df_raw.count()}")

# COMMAND ----------

# 2. 정제 (Cleansing)
df_cleaned = (df_raw
    # NULL 키 제거
    .filter(col(merge_keys[0]).isNotNull())
    # 타입 캐스팅
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("decimal(18,2)"))
    # 문자열 정리
    .withColumn("status", upper(trim(col("status"))))
    # 중복 제거 (같은 키의 최신 레코드만)
    .dropDuplicates(merge_keys)
    # Silver 메타 컬럼
    .withColumn("_updated_at", current_timestamp())
)

print(f"정제 후 레코드 수: {df_cleaned.count()}")

# COMMAND ----------

# 3. MERGE (Upsert) into Silver
if spark.catalog.tableExists(target_table):
    dt = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])

    (dt.alias("t")
        .merge(df_cleaned.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ MERGE 완료: {target_table}")
else:
    # 테이블이 없으면 최초 생성
    df_cleaned.write.format("delta").saveAsTable(target_table)
    print(f"✅ 테이블 최초 생성: {target_table}")
