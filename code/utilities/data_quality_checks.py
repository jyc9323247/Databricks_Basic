# Databricks notebook source
# MAGIC %md
# MAGIC # DQ (Data Quality) 체크 함수 모음
# MAGIC > 파이프라인 중간에 삽입하여 데이터 품질을 검증합니다.

from pyspark.sql.functions import col, count, when, isnan, sum as _sum

# ============================================================
# 1. NULL 비율 체크
# ============================================================

def check_null_ratio(df, columns=None, threshold=0.05):
    """
    지정 컬럼의 NULL 비율을 확인합니다.

    Args:
        columns: 검사할 컬럼 리스트 (None이면 전체)
        threshold: 허용 NULL 비율 (기본 5%)

    Returns:
        (pass: bool, report: DataFrame)
    """
    cols = columns or df.columns
    total = df.count()
    if total == 0:
        print("⚠️ DataFrame이 비어 있습니다.")
        return False, None

    null_exprs = [
        (_sum(when(col(c).isNull(), 1).otherwise(0)) / total).alias(c)
        for c in cols
    ]
    report = df.select(*null_exprs)
    report_row = report.collect()[0]

    all_pass = True
    for c in cols:
        ratio = report_row[c]
        status = "✅" if ratio <= threshold else "🔴"
        if ratio > threshold:
            all_pass = False
        print(f"  {status} {c}: NULL {ratio:.2%} (임계값: {threshold:.0%})")

    return all_pass, report


# ============================================================
# 2. 유니크 키 검증
# ============================================================

def check_unique_key(df, key_columns):
    """비즈니스 키가 유니크한지 검증합니다."""
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    is_unique = total == distinct

    if is_unique:
        print(f"  ✅ 유니크 키 검증 통과: {key_columns} ({total} rows, 전부 유니크)")
    else:
        dup_count = total - distinct
        print(f"  🔴 중복 발견: {key_columns} (전체 {total}, 유니크 {distinct}, 중복 {dup_count})")

    return is_unique


# ============================================================
# 3. 행 수 범위 검증
# ============================================================

def check_row_count(df, min_rows=1, max_rows=None):
    """행 수가 예상 범위 내인지 확인합니다."""
    total = df.count()
    pass_min = total >= min_rows
    pass_max = total <= max_rows if max_rows else True
    is_pass = pass_min and pass_max

    if is_pass:
        print(f"  ✅ 행 수 검증 통과: {total} rows (범위: {min_rows}~{max_rows or '∞'})")
    else:
        print(f"  🔴 행 수 검증 실패: {total} rows (범위: {min_rows}~{max_rows or '∞'})")

    return is_pass


# ============================================================
# 4. 값 범위 검증
# ============================================================

def check_value_range(df, column, min_val=None, max_val=None):
    """특정 컬럼의 값이 범위 내인지 확인합니다."""
    stats = df.select(
        _sum(when(col(column) < min_val, 1).otherwise(0)).alias("below") if min_val is not None else count("*").alias("below"),
        _sum(when(col(column) > max_val, 1).otherwise(0)).alias("above") if max_val is not None else count("*").alias("above"),
    ).collect()[0]

    violations = 0
    if min_val is not None:
        violations += stats["below"]
    if max_val is not None:
        violations += stats["above"]

    if violations == 0:
        print(f"  ✅ 값 범위 검증 통과: {column} (범위: {min_val}~{max_val})")
    else:
        print(f"  🔴 값 범위 위반: {column} — {violations}건 (범위: {min_val}~{max_val})")

    return violations == 0


# ============================================================
# 5. 통합 DQ 실행기
# ============================================================

def run_dq_checks(df, checks):
    """
    여러 DQ 체크를 한 번에 실행합니다.

    Args:
        df: 검사 대상 DataFrame
        checks: 체크 리스트, 예:
            [
                {"type": "null", "columns": ["id", "name"], "threshold": 0.01},
                {"type": "unique", "key_columns": ["id"]},
                {"type": "row_count", "min_rows": 100},
                {"type": "range", "column": "amount", "min_val": 0},
            ]

    Returns:
        전체 통과 여부 (bool)
    """
    all_pass = True
    for i, chk in enumerate(checks):
        print(f"\n--- DQ Check #{i+1}: {chk['type']} ---")
        if chk["type"] == "null":
            passed, _ = check_null_ratio(df, chk.get("columns"), chk.get("threshold", 0.05))
        elif chk["type"] == "unique":
            passed = check_unique_key(df, chk["key_columns"])
        elif chk["type"] == "row_count":
            passed = check_row_count(df, chk.get("min_rows", 1), chk.get("max_rows"))
        elif chk["type"] == "range":
            passed = check_value_range(df, chk["column"], chk.get("min_val"), chk.get("max_val"))
        else:
            print(f"  ⚠️ 알 수 없는 체크 타입: {chk['type']}")
            passed = True

        if not passed:
            all_pass = False

    print(f"\n{'='*40}")
    print(f"{'✅ 전체 DQ 통과' if all_pass else '🔴 DQ 실패 — 파이프라인 중단 검토'}")
    return all_pass
