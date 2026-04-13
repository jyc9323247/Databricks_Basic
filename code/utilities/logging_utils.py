# Databricks notebook source
# MAGIC %md
# MAGIC # 작업 로깅 유틸리티
# MAGIC > ETL 파이프라인 실행 이력을 Delta 테이블에 기록합니다.

from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime

# ============================================================
# 로그 테이블 스키마
# ============================================================

LOG_TABLE = "catalog.ops.pipeline_log"

LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("step_name", StringType(), False),
    StructField("status", StringType(), False),       # STARTED, SUCCESS, FAILED
    StructField("row_count", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("finished_at", TimestampType(), True),
])


# ============================================================
# 로깅 함수
# ============================================================

def log_start(run_id, pipeline_name, step_name):
    """파이프라인 스텝 시작을 기록합니다."""
    row = [(run_id, pipeline_name, step_name, "STARTED", None, None, datetime.now(), None)]
    df = spark.createDataFrame(row, LOG_SCHEMA)
    df.write.mode("append").saveAsTable(LOG_TABLE)
    print(f"📝 [{step_name}] STARTED")


def log_success(run_id, pipeline_name, step_name, row_count=None):
    """파이프라인 스텝 성공을 기록합니다."""
    row = [(run_id, pipeline_name, step_name, "SUCCESS", row_count, None, None, datetime.now())]
    df = spark.createDataFrame(row, LOG_SCHEMA)
    df.write.mode("append").saveAsTable(LOG_TABLE)
    print(f"✅ [{step_name}] SUCCESS (rows: {row_count})")


def log_failure(run_id, pipeline_name, step_name, error_message):
    """파이프라인 스텝 실패를 기록합니다."""
    row = [(run_id, pipeline_name, step_name, "FAILED", None, str(error_message)[:2000], None, datetime.now())]
    df = spark.createDataFrame(row, LOG_SCHEMA)
    df.write.mode("append").saveAsTable(LOG_TABLE)
    print(f"🔴 [{step_name}] FAILED: {error_message}")


# ============================================================
# 래퍼 함수 (try-except 자동화)
# ============================================================

def run_with_logging(run_id, pipeline_name, step_name, func, *args, **kwargs):
    """
    함수를 실행하면서 자동으로 로깅합니다.

    Args:
        run_id: 실행 ID
        pipeline_name: 파이프라인 이름
        step_name: 스텝 이름
        func: 실행할 함수
        *args, **kwargs: 함수에 전달할 인자

    Returns:
        함수 실행 결과
    """
    log_start(run_id, pipeline_name, step_name)
    try:
        result = func(*args, **kwargs)
        row_count = result if isinstance(result, int) else None
        log_success(run_id, pipeline_name, step_name, row_count)
        return result
    except Exception as e:
        log_failure(run_id, pipeline_name, step_name, e)
        raise


# ============================================================
# 사용 예시
# ============================================================

# import uuid
# run_id = str(uuid.uuid4())
#
# def my_etl_step():
#     df = spark.read.table("catalog.bronze.orders")
#     df_clean = df.filter(col("status").isNotNull())
#     df_clean.write.mode("overwrite").saveAsTable("catalog.silver.orders")
#     return df_clean.count()
#
# run_with_logging(run_id, "daily_sales_pipeline", "bronze_to_silver", my_etl_step)
