# 📥 06. 데이터 수집 — Auto Loader, COPY INTO, Streaming

> 데이터를 Lakehouse에 넣는 방법. Auto Loader가 실무의 80%를 차지합니다.

---

## 1. 수집 방법 의사결정 기준

| 기준 | Auto Loader | COPY INTO | Structured Streaming |
|------|-------------|-----------|---------------------|
| 파일 추적 | ✅ 자동 (체크포인트) | ✅ 파일 상태 추적 | ✅ 체크포인트 기반 |
| 스키마 추론/진화 | ✅ 자동 | ❌ 수동 지정 | ⚠️ 제한적 |
| 수백만 파일 | ✅ 효율적 (incremental listing) | ❌ 느림 (매번 전체 목록) | ✅ |
| 일회성 로드 | ⚠️ 과도 | ✅ 적합 | ❌ 과도 |
| 실시간 스트리밍 | ✅ near real-time | ❌ 배치 전용 | ✅ 실시간 |

**결론**: 대부분의 파일 수집은 **Auto Loader**, 일회성 벌크 로드는 **COPY INTO**

---

## 2. Auto Loader (`cloudFiles`)

### 기본 패턴

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")                    # csv, parquet, json, avro 등
    .option("cloudFiles.schemaLocation", "/path/to/schema") # 스키마 저장 위치
    .load("/path/to/source/files/")
)

(df.writeStream
    .format("delta")
    .option("checkpointLocation", "/path/to/checkpoint")
    .outputMode("append")
    .trigger(availableNow=True)  # 현재 쌓인 파일만 처리 후 종료 (배치처럼 사용)
    .toTable("catalog.bronze.raw_events")
)
```

### 핵심 옵션

| 옵션 | 설명 | 예시 |
|------|------|------|
| `cloudFiles.format` | 소스 파일 포맷 | `"json"`, `"csv"`, `"parquet"` |
| `cloudFiles.schemaLocation` | 추론된 스키마 저장 경로 | DBFS 또는 클라우드 경로 |
| `cloudFiles.schemaEvolutionMode` | 새 컬럼 발견 시 동작 | `"addNewColumns"` (기본), `"none"`, `"rescue"` |
| `cloudFiles.inferColumnTypes` | 타입 자동 추론 | `"true"` (CSV일 때 권장) |
| `cloudFiles.schemaHints` | 특정 컬럼 타입 지정 | `"id BIGINT, amount DOUBLE"` |

### CSV 수집 예시 (한국어 데이터)

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("encoding", "UTF-8")       # 또는 "EUC-KR"
    .option("sep", ",")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
```

### Trigger 옵션

| Trigger | 용도 |
|---------|------|
| `trigger(availableNow=True)` | 쌓인 파일만 처리 후 종료 (**실무에서 가장 많이 사용**) |
| `trigger(processingTime="1 minute")` | 1분 간격 마이크로 배치 |
| `trigger(once=True)` | 한 번만 실행 후 종료 (deprecated, `availableNow` 권장) |
| 미지정 | 연속 실행 (실시간) |

### 메타 컬럼 추가 (권장)

```python
from pyspark.sql.functions import current_timestamp, input_file_name

df = df.withColumn("_ingested_at", current_timestamp()) \
       .withColumn("_source_file", input_file_name())
```

---

## 3. COPY INTO (일회성/벌크 로드)

```sql
COPY INTO catalog.bronze.raw_orders
FROM '/path/to/source/files/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true',
    'encoding' = 'UTF-8'
)
COPY_OPTIONS ('mergeSchema' = 'true');
```

### Auto Loader와의 차이

| 항목 | Auto Loader | COPY INTO |
|------|-------------|-----------|
| 멱등성 | ✅ 체크포인트 기반 | ✅ 파일 상태 추적 |
| 파일 탐색 | Incremental (빠름) | 매번 전체 목록 (느림) |
| 스키마 진화 | ✅ 자동 | ⚠️ 수동 옵션 |
| 대규모 파일 | ✅ 적합 | ❌ 성능 저하 |
| 설정 복잡도 | 중간 | 낮음 |

---

## 4. Structured Streaming 기본

### 기본 구조

```python
# 읽기 (스트림)
df_stream = spark.readStream \
    .format("delta") \
    .table("catalog.bronze.raw_events")

# 변환
df_transformed = df_stream \
    .filter(col("event_type").isNotNull()) \
    .withColumn("processed_at", current_timestamp())

# 쓰기 (스트림)
query = (df_transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="5 minutes")
    .toTable("catalog.silver.processed_events")
)
```

### Output Mode

| 모드 | 설명 | 용도 |
|------|------|------|
| `append` | 새 행만 추가 | 대부분의 ETL |
| `complete` | 전체 결과 덮어쓰기 | 집계 쿼리 |
| `update` | 변경된 행만 업데이트 | 워터마크 기반 집계 |

### 체크포인트 주의사항

- 체크포인트 = 스트리밍 진행 상태 저장소
- **절대 삭제하지 말 것** → 삭제하면 처음부터 재처리 (데이터 중복/유실)
- **경로 변경하지 말 것** → 새 체크포인트 = 처음부터 재처리

---

## 5. 증분 수집 패턴 (Auto Loader + MERGE)

### CDC 스타일 수집

```python
# 1. Auto Loader로 Bronze 수집
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
)

# 2. foreachBatch로 Silver MERGE
def upsert_to_silver(batch_df, batch_id):
    from delta.tables import DeltaTable

    if batch_df.isEmpty():
        return

    dt = DeltaTable.forName(spark, "catalog.silver.customers")
    (dt.alias("t")
        .merge(batch_df.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

(df_raw.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)
```

---

## 6. 외부 소스 연결

### JDBC (Oracle, SQL Server, PostgreSQL)

```python
df = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@host:1521:orcl") \
    .option("dbtable", "(SELECT * FROM orders WHERE order_date >= '2025-01-01') subq") \
    .option("user", dbutils.secrets.get("scope", "db_user")) \
    .option("password", dbutils.secrets.get("scope", "db_password")) \
    .option("fetchsize", "10000") \
    .option("partitionColumn", "order_id") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .option("numPartitions", "10") \
    .load()
```

### 병렬 JDBC 읽기 핵심 옵션

| 옵션 | 설명 |
|------|------|
| `partitionColumn` | 분할 기준 컬럼 (숫자/날짜) |
| `lowerBound` / `upperBound` | 분할 범위 |
| `numPartitions` | 병렬 커넥션 수 (소스 DB 부하 고려) |
| `fetchsize` | 한 번에 가져올 행 수 |

---

## 7. 핵심 요약

- **Auto Loader가 기본 선택** — 스키마 진화, 증분 탐색, 스트리밍 모두 지원
- `trigger(availableNow=True)` = "배치처럼 쓰는 스트리밍" (가장 흔한 패턴)
- COPY INTO는 일회성 벌크 로드에만 사용
- 체크포인트는 **절대 삭제/이동 금지**
- Bronze 수집 시 `_ingested_at`, `_source_file` 메타 컬럼 추가 습관화
- JDBC 병렬 읽기 시 소스 DB 부하 고려 (`numPartitions` 조절)
