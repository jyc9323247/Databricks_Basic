# Databricks notebook source
# MAGIC %md
# MAGIC # 데이터 Skew 해소 패턴
# MAGIC > Salting 기법으로 JOIN 키 쏠림 문제를 해결합니다.

from pyspark.sql.functions import col, concat, lit, floor, rand, explode, array, count

# ============================================================
# 1. Skew 진단
# ============================================================

def diagnose_skew(df, key_column, top_n=20):
    """키별 건수 분포를 확인하여 Skew 여부를 판단합니다."""
    dist = (df.groupBy(key_column)
        .agg(count("*").alias("cnt"))
        .orderBy(col("cnt").desc())
    )
    print(f"=== {key_column} 분포 (상위 {top_n}) ===")
    dist.show(top_n, truncate=False)

    stats = dist.select(
        avg("cnt").alias("avg"),
        max("cnt").alias("max"),
        min("cnt").alias("min"),
    ).collect()[0]

    skew_ratio = stats["max"] / stats["avg"] if stats["avg"] > 0 else 0
    print(f"평균: {stats['avg']:.0f}, 최대: {stats['max']}, 비율(max/avg): {skew_ratio:.1f}x")
    if skew_ratio > 10:
        print("⚠️ Skew 가능성 높음 — Salting 또는 AQE Skew Join 권장")
    return dist


# ============================================================
# 2. Salting Join
# ============================================================

def salted_join(big_df, small_df, join_key, salt_buckets=10, how="inner"):
    """
    Salting 기법으로 Skew JOIN을 해소합니다.

    Args:
        big_df: 큰 쪽 DataFrame (skew 있는 쪽)
        small_df: 작은 쪽 DataFrame
        join_key: 조인 키 컬럼명
        salt_buckets: 솔트 버킷 수 (기본 10)
        how: 조인 타입 (inner, left 등)

    Returns:
        Salted Join 결과 DataFrame (salt 컬럼 제거됨)
    """
    salted_key = f"_salted_{join_key}"

    # 큰 테이블: 랜덤 솔트 추가
    big_salted = (big_df
        .withColumn("_salt", floor(rand() * salt_buckets).cast("int"))
        .withColumn(salted_key, concat(col(join_key).cast("string"), lit("_"), col("_salt")))
    )

    # 작은 테이블: 모든 솔트 값으로 복제
    small_salted = (small_df
        .withColumn("_salt", explode(array([lit(i) for i in range(salt_buckets)])))
        .withColumn(salted_key, concat(col(join_key).cast("string"), lit("_"), col("_salt")))
    )

    # Salted Join
    result = big_salted.join(small_salted, salted_key, how)

    # 정리: salt 관련 컬럼 제거, 중복 키 컬럼 제거
    drop_cols = ["_salt", salted_key]
    # 양쪽에 동일 컬럼이 있으면 한쪽 제거
    for c in small_df.columns:
        if c in big_df.columns and c != join_key:
            pass  # 필요 시 alias로 구분

    return result.drop(*drop_cols)


# ============================================================
# 사용 예시
# ============================================================

# 1) 진단
# diagnose_skew(orders_df, "customer_id")

# 2) Salting Join
# result = salted_join(orders_df, customers_df, "customer_id", salt_buckets=20)
