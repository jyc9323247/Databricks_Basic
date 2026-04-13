# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze 레이어 Auto Loader 수집 패턴
# MAGIC > 소스 데이터를 있는 그대로 Bronze 테이블에 적재합니다.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# COMMAND ----------

# 파라미터
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("schema_path", "")
dbutils.widgets.dropdown("file_format", "json", ["json", "csv", "parquet", "avro"])

source_path = dbutils.widgets.get("source_path")
target_table = dbutils.widgets.get("target_table")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
schema_path = dbutils.widgets.get("schema_path")
file_format = dbutils.widgets.get("file_format")

# COMMAND ----------

# Auto Loader 읽기
reader = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", file_format)
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
)

# CSV 전용 옵션
if file_format == "csv":
    reader = (reader
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("encoding", "UTF-8")
    )

df_raw = reader.load(source_path)

# COMMAND ----------

# 메타 컬럼 추가
df_bronze = (df_raw
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# COMMAND ----------

# Bronze 테이블에 적재 (availableNow = 배치처럼 사용)
query = (df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

query.awaitTermination()
print(f"✅ Bronze 적재 완료: {target_table}")
