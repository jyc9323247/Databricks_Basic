# Databricks notebook source
# MAGIC %md
# MAGIC # 중복 제거 패턴
# MAGIC > Window 함수 기반으로 비즈니스 키 기준 최신 레코드만 남깁니다.

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

def deduplicate(df, key_columns, order_column, ascending=False):
    """
    중복 제거 함수.

    Args:
        df: 원본 DataFrame
        key_columns: 중복 판단 기준 컬럼 리스트 (예: ["order_id"])
        order_column: 정렬 기준 컬럼 (최신 기준, 예: "updated_at")
        ascending: True면 가장 오래된 것, False면 가장 최신 것(기본)

    Returns:
        중복 제거된 DataFrame
    """
    order_col = col(order_column).asc() if ascending else col(order_column).desc()

    w = Window.partitionBy(*key_columns).orderBy(order_col)

    return (df
        .withColumn("_row_num", row_number().over(w))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


# 사용 예시
# df_deduped = deduplicate(df_raw, ["order_id"], "updated_at")
# df_deduped = deduplicate(df_raw, ["customer_id", "order_date"], "created_at")
