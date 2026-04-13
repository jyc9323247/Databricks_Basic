# 🔥 02. Spark DataFrame API 실무 패턴

> DataFrame API의 20%만 알면 실무 변환 작업의 80%를 처리할 수 있습니다.

---

## 1. SparkSession 기본

Databricks Notebook에서는 `spark` 변수가 자동 생성됩니다. 별도 생성 불필요.

```python
# Databricks에서는 이미 존재
# spark = SparkSession.builder.getOrCreate()  # 로컬 환경에서만 필요

# 현재 설정 확인
spark.conf.get("spark.sql.shuffle.partitions")
spark.version  # Spark 버전 확인
```

---

## 2. 데이터 읽기 (`spark.read`)

### 포맷별 읽기

```python
# Delta (가장 많이 씀)
df = spark.read.table("catalog.schema.my_table")
df = spark.read.format("delta").load("/path/to/delta/table")

# CSV
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("encoding", "UTF-8") \
    .load("/path/to/file.csv")

# Parquet
df = spark.read.parquet("/path/to/file.parquet")

# JSON
df = spark.read.json("/path/to/file.json")

# JDBC (외부 DB)
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("dbtable", "public.users") \
    .option("user", "username") \
    .option("password", dbutils.secrets.get("scope", "pg_password")) \
    .load()
```

### 읽기 시 자주 쓰는 옵션

| 옵션 | 용도 | 예시 |
|------|------|------|
| `header` | CSV 첫 행을 컬럼명으로 | `"true"` |
| `inferSchema` | 타입 자동 추론 | `"true"` (소규모만, 대규모는 스키마 명시) |
| `mergeSchema` | 스키마 병합 (Delta/Parquet) | `"true"` |
| `encoding` | 문자 인코딩 | `"UTF-8"`, `"EUC-KR"` |
| `multiLine` | JSON 여러 줄 | `"true"` |
| `sep` | CSV 구분자 | `","`, `"\t"`, `"|"` |

---

## 3. 변환 Must-Know 20개

### 3-1. 컬럼 선택 & 생성

```python
from pyspark.sql.functions import col, lit, when, concat, upper, lower, trim

# 컬럼 선택
df.select("name", "age")
df.select(col("name"), col("age") + 1)

# 컬럼 추가
df.withColumn("age_group", when(col("age") < 30, "young").otherwise("senior"))
df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df.withColumn("is_active", lit(True))

# 컬럼 삭제
df.drop("temp_col", "debug_col")

# 컬럼 이름 변경
df.withColumnRenamed("old_name", "new_name")
df.select(col("old_name").alias("new_name"))
```

### 3-2. 필터링

```python
# 기본 필터
df.filter(col("age") >= 18)
df.where(col("status") == "active")  # filter와 동일

# 복합 조건
df.filter((col("age") >= 18) & (col("country") == "KR"))
df.filter((col("status") == "A") | (col("status") == "B"))

# IN 조건
df.filter(col("status").isin("A", "B", "C"))

# NULL 처리
df.filter(col("email").isNotNull())
df.filter(col("phone").isNull())

# LIKE
df.filter(col("name").like("%김%"))
df.filter(col("email").rlike(r"^[a-zA-Z0-9+_.-]+@.+$"))  # 정규식
```

### 3-3. 집계 (GROUP BY)

```python
from pyspark.sql.functions import count, sum, avg, min, max, countDistinct, collect_list, collect_set

# 기본 집계
df.groupBy("department").agg(
    count("*").alias("emp_count"),
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary"),
    countDistinct("job_title").alias("unique_titles")
)

# 여러 그룹핑 키
df.groupBy("year", "month").agg(
    sum("revenue").alias("total_revenue")
)

# 리스트/집합 수집 (주의: 대용량이면 OOM 위험)
df.groupBy("order_id").agg(
    collect_list("product_name").alias("products"),
    collect_set("category").alias("unique_categories")
)
```

### 3-4. JOIN

```python
# Inner Join (기본)
result = orders.join(customers, orders.customer_id == customers.id, "inner")

# Left Join
result = orders.join(customers, "customer_id", "left")

# Anti Join (왼쪽에만 있는 것 = NOT IN)
new_customers = today_customers.join(yesterday_customers, "customer_id", "left_anti")

# Semi Join (교집합 키만, 오른쪽 컬럼 안 가져옴 = IN)
existing = orders.join(valid_products, "product_id", "left_semi")

# Cross Join (주의: 결과 폭발)
combinations = sizes.crossJoin(colors)
```

**JOIN 타입 요약**:

| 타입 | 설명 | 실무 용도 |
|------|------|-----------|
| `inner` | 양쪽 키 매칭만 | 기본 조인 |
| `left` | 왼쪽 전체 + 매칭 | 누락 데이터 포함할 때 |
| `left_anti` | 왼쪽에만 있는 것 | 신규/누락 탐지 |
| `left_semi` | 왼쪽 중 매칭된 것만 | 필터 용도 (IN 대체) |
| `full` | 양쪽 전체 | 데이터 비교, reconciliation |

### 3-5. 정렬 & 중복 제거

```python
# 정렬
df.orderBy("created_at")                      # 오름차순
df.orderBy(col("created_at").desc())           # 내림차순
df.orderBy("department", col("salary").desc()) # 복합 정렬

# 중복 제거
df.distinct()                                  # 전체 행 기준
df.dropDuplicates(["email"])                   # 특정 컬럼 기준
df.dropDuplicates(["customer_id", "order_date"])
```

### 3-6. NULL 처리

```python
from pyspark.sql.functions import coalesce

# NULL 채우기
df.fillna(0, subset=["amount"])
df.fillna({"amount": 0, "status": "unknown"})
df.na.fill(0)

# NULL이면 다른 컬럼 값 사용
df.withColumn("phone", coalesce(col("mobile"), col("home_phone"), lit("N/A")))

# NULL 행 제거
df.dropna()                          # 하나라도 NULL이면 제거
df.dropna(subset=["name", "email"])  # 특정 컬럼만
```

### 3-7. Window 함수

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum as _sum

# 윈도우 정의
w = Window.partitionBy("department").orderBy(col("salary").desc())

# 순위
df.withColumn("rank", row_number().over(w))
df.withColumn("rank", rank().over(w))        # 동점 허용
df.withColumn("rank", dense_rank().over(w))  # 동점 허용, 건너뛰기 없음

# 이전/다음 값
w_time = Window.partitionBy("customer_id").orderBy("order_date")
df.withColumn("prev_order_date", lag("order_date", 1).over(w_time))
df.withColumn("next_order_date", lead("order_date", 1).over(w_time))

# 누적 합계
w_cumsum = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", _sum("amount").over(w_cumsum))
```

### 3-8. 문자열 처리

```python
from pyspark.sql.functions import split, regexp_extract, regexp_replace, substring, length, trim, ltrim, rtrim

# 분리
df.withColumn("domain", split(col("email"), "@")[1])

# 정규식 추출
df.withColumn("year", regexp_extract(col("date_str"), r"(\d{4})-\d{2}-\d{2}", 1))

# 치환
df.withColumn("clean_phone", regexp_replace(col("phone"), r"[^0-9]", ""))

# 부분 문자열
df.withColumn("area_code", substring(col("phone"), 1, 3))
```

### 3-9. 날짜/시간 처리

```python
from pyspark.sql.functions import (
    current_timestamp, current_date, to_date, to_timestamp,
    date_add, date_sub, datediff, months_between,
    year, month, dayofweek, date_format
)

# 변환
df.withColumn("order_date", to_date(col("date_str"), "yyyy-MM-dd"))
df.withColumn("order_ts", to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"))

# 연산
df.withColumn("delivery_date", date_add(col("order_date"), 3))
df.withColumn("days_since", datediff(current_date(), col("order_date")))

# 추출
df.withColumn("order_year", year(col("order_date")))
df.withColumn("order_month", month(col("order_date")))

# 포맷팅
df.withColumn("formatted", date_format(col("order_date"), "yyyy/MM/dd"))
```

### 3-10. 피벗 & 배열 처리

```python
from pyspark.sql.functions import explode, array, struct

# Pivot (행 → 열)
df.groupBy("customer_id").pivot("product_category").agg(sum("amount"))

# Explode (배열 → 행)
df.withColumn("tag", explode(col("tags")))

# Union
df1.unionByName(df2, allowMissingColumns=True)  # 컬럼 이름 기준 합치기
```

---

## 4. 데이터 쓰기

```python
# Delta 테이블로 저장
df.write.mode("overwrite").saveAsTable("catalog.schema.my_table")
df.write.mode("append").saveAsTable("catalog.schema.my_table")

# 파티션 분할 저장
df.write.mode("overwrite") \
    .partitionBy("event_date") \
    .saveAsTable("catalog.schema.events")

# 파일로 저장
df.write.mode("overwrite").parquet("/path/to/output/")
df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("/path/to/output.csv")
```

### Write Mode 정리

| 모드 | 동작 |
|------|------|
| `overwrite` | 기존 데이터 전체 삭제 후 쓰기 |
| `append` | 기존 데이터에 추가 |
| `ignore` | 테이블 존재하면 아무 것도 안 함 |
| `error` / `errorifexists` | 테이블 존재하면 에러 (기본값) |

---

## 5. PySpark vs Spark SQL 비교

같은 작업을 양쪽으로 할 수 있고, 실무에서는 혼용합니다.

```python
# PySpark DataFrame API
result = (df
    .filter(col("status") == "active")
    .groupBy("department")
    .agg(count("*").alias("cnt"))
    .orderBy(col("cnt").desc())
)

# Spark SQL (동일 결과)
df.createOrReplaceTempView("employees")
result = spark.sql("""
    SELECT department, COUNT(*) AS cnt
    FROM employees
    WHERE status = 'active'
    GROUP BY department
    ORDER BY cnt DESC
""")
```

---

## 6. UDF — 언제 쓰고, 왜 피해야 하는가

### UDF 사용법

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def mask_email(email):
    if email is None:
        return None
    parts = email.split("@")
    return parts[0][:2] + "****@" + parts[1]

df.withColumn("masked_email", mask_email(col("email")))
```

### UDF를 피해야 하는 이유

- **직렬화 오버헤드**: 데이터가 JVM → Python → JVM 왕복 (10~100배 느림)
- **Catalyst 최적화 불가**: Spark 옵티마이저가 UDF 내부를 이해 못 함
- **대안**: 내장 함수로 해결 가능한 경우가 대부분

```python
# ❌ UDF로 이메일 마스킹
@udf(returnType=StringType())
def mask_email(email): ...

# ✅ 내장 함수로 동일 작업
from pyspark.sql.functions import concat, substring, split, lit
df.withColumn("masked_email",
    concat(substring(col("email"), 1, 2), lit("****@"), split(col("email"), "@")[1])
)
```

---

## 7. 핵심 요약

- `spark.read` → 변환 체인 → `write` 가 기본 흐름
- **변환 20개**를 익히면 대부분의 ETL 작업 가능
- JOIN 시 타입(inner/left/anti/semi)을 용도에 맞게 선택
- Window 함수는 순위, 이전/다음 값, 누적 계산에 필수
- UDF는 최후의 수단 — 내장 함수를 먼저 찾을 것
