# Databricks notebook source
# MAGIC %md
# MAGIC # Broadcast Join 적용 패턴
# MAGIC > 작은 테이블을 메모리에 올려 Shuffle 없이 JOIN합니다.

from pyspark.sql.functions import broadcast, col

# ============================================================
# 1. 테이블 크기 확인 유틸리티
# ============================================================

def check_table_size_mb(table_name):
    """테이블의 대략적인 크기(MB)를 확인합니다."""
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
    size_mb = detail["sizeInBytes"] / (1024 * 1024)
    num_files = detail["numFiles"]
    print(f"📊 {table_name}: {size_mb:.1f} MB, {num_files} files")
    return size_mb


# ============================================================
# 2. Broadcast Join 패턴
# ============================================================

def smart_join(big_df, small_df, join_key, how="inner", broadcast_threshold_mb=100):
    """
    작은 테이블 크기를 판단하여 자동으로 Broadcast Join을 적용합니다.

    Args:
        big_df: 큰 DataFrame
        small_df: 작은 DataFrame (Broadcast 대상)
        join_key: JOIN 키 (문자열 또는 조건)
        how: JOIN 타입
        broadcast_threshold_mb: Broadcast 적용 기준 (MB)
    """
    # 크기 추정 (캐시되지 않은 경우 정확하지 않을 수 있음)
    try:
        size_bytes = spark.sessionState.executePlan(
            small_df._jdf.queryExecution().logical()
        ).optimizedPlan().stats().sizeInBytes()
        size_mb = size_bytes / (1024 * 1024)
    except Exception:
        # 추정 실패 시 그냥 broadcast 적용
        size_mb = 0
        print("⚠️ 크기 추정 불가 — broadcast 적용")

    if size_mb <= broadcast_threshold_mb:
        print(f"✅ Broadcast Join 적용 (작은 테이블: ~{size_mb:.1f} MB)")
        return big_df.join(broadcast(small_df), join_key, how)
    else:
        print(f"ℹ️ SortMerge Join 적용 (작은 테이블: ~{size_mb:.1f} MB > {broadcast_threshold_mb} MB)")
        return big_df.join(small_df, join_key, how)


# ============================================================
# 3. Broadcast 임계값 설정
# ============================================================

def set_broadcast_threshold(mb=100):
    """Spark의 자동 Broadcast 임계값을 변경합니다."""
    bytes_val = mb * 1024 * 1024
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", bytes_val)
    print(f"✅ autoBroadcastJoinThreshold = {mb} MB ({bytes_val} bytes)")


# ============================================================
# 사용 예시
# ============================================================

# 방법 1: 직접 broadcast() 명시
# result = orders.join(broadcast(products), "product_id")

# 방법 2: SQL 힌트
# spark.sql("""
#     SELECT /*+ BROADCAST(products) */ *
#     FROM orders JOIN products USING (product_id)
# """)

# 방법 3: 전역 임계값 조정
# set_broadcast_threshold(200)  # 200MB 이하 자동 broadcast
