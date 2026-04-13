# Databricks notebook source
# MAGIC %md
# MAGIC # 스키마 진화 처리 패턴
# MAGIC > 소스 스키마 변경에 안전하게 대응하는 방법

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType

def safe_schema_write(df, target_table, mode="append"):
    """
    스키마 변경을 감지하고 안전하게 처리하는 쓰기 함수.
    - 새 컬럼 추가: mergeSchema로 자동 처리
    - 컬럼 삭제: 기존 스키마 기준으로 맞춤 (NULL 채움)
    - 타입 변경: 에러 발생 → 수동 처리 필요
    """
    try:
        if spark.catalog.tableExists(target_table):
            target_schema = spark.read.table(target_table).schema
            source_cols = set(df.columns)
            target_cols = set(field.name for field in target_schema.fields)

            # 소스에만 있는 컬럼 (새로 추가된 것)
            new_cols = source_cols - target_cols
            if new_cols:
                print(f"⚠️ 새 컬럼 감지: {new_cols} → mergeSchema 적용")

            # 타겟에만 있는 컬럼 (소스에서 삭제된 것)
            missing_cols = target_cols - source_cols
            if missing_cols:
                print(f"⚠️ 누락 컬럼 감지: {missing_cols} → NULL로 채움")
                for mc in missing_cols:
                    target_type = [f.dataType for f in target_schema.fields if f.name == mc][0]
                    df = df.withColumn(mc, lit(None).cast(target_type))

        # 쓰기
        (df.write
            .mode(mode)
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )
        print(f"✅ 쓰기 완료: {target_table}")

    except Exception as e:
        if "schema mismatch" in str(e).lower() or "cannot cast" in str(e).lower():
            print(f"🔴 타입 변경 감지 — 수동 처리 필요: {e}")
            raise
        raise


# 사용 예시
# safe_schema_write(df_new_data, "catalog.silver.orders", mode="append")
