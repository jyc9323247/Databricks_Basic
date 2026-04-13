# 🏗️ 01. Lakehouse 아키텍처 & Medallion Architecture

> Databricks를 이해하려면 "왜 Lakehouse인가?"부터 시작해야 합니다.

---

## 1. Lakehouse란?

**한 줄 정의**: Data Warehouse의 **트랜잭션 안정성(ACID)**과 Data Lake의 **확장성·유연성**을 하나의 플랫폼에서 구현한 아키텍처.

### 전통 아키텍처의 문제

| 아키텍처 | 장점 | 한계 |
|----------|------|------|
| **Data Warehouse** (Oracle, Teradata) | ACID, SQL 최적화, 거버넌스 | 비싸다, 비정형 데이터 불가, 스케일 한계 |
| **Data Lake** (HDFS, S3) | 저렴, 모든 데이터 저장 가능 | 품질 관리 어려움, ACID 없음, "Data Swamp" 위험 |
| **Lambda (Lake + DW 이중)** | 각각의 장점 활용 | 이중 관리, 데이터 불일치, 복잡한 파이프라인 |

### Lakehouse가 해결하는 것

```
저렴한 Object Storage (S3, ADLS, GCS)
    + Delta Lake (ACID 트랜잭션 계층)
    + Unity Catalog (거버넌스)
    + Photon Engine (고속 SQL 엔진)
= 하나의 플랫폼에서 ETL + BI + ML 전부 가능
```

---

## 2. Medallion Architecture (Bronze → Silver → Gold)

Lakehouse에서 데이터 품질을 단계별로 올려가는 **계층형 설계 패턴**.

### 각 레이어 요약

| 레이어 | 별칭 | 목적 | 데이터 품질 | 저장 포맷 |
|--------|------|------|-------------|-----------|
| **Bronze** | Raw / Landing | 원본 그대로 적재 | 낮음 (있는 그대로) | Delta (원본 보존) |
| **Silver** | Cleaned / Conformed | 정제, 중복 제거, 타입 보정 | 중간 (정규화됨) | Delta |
| **Gold** | Curated / Business | 비즈니스 집계, KPI, 리포트용 | 높음 (바로 소비 가능) | Delta |

### 각 레이어의 역할 상세

#### Bronze (원본 적재)
- 소스 시스템에서 **있는 그대로** 가져옴 (스키마 변환 최소화)
- `_ingested_at`, `_source_file` 등 메타 컬럼 추가
- **목적**: 원본 데이터 보존 (문제 발생 시 Silver부터 재처리 가능)
- Auto Loader 또는 COPY INTO로 수집

```python
# Bronze 적재 예시
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(source_path)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

df_raw.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .toTable("catalog.bronze.raw_orders")
```

#### Silver (정제/변환)
- 타입 캐스팅, NULL 처리, 중복 제거
- 비즈니스 키 기준 **MERGE (Upsert)**
- 컬럼 이름 표준화 (`camelCase` → `snake_case` 등)
- 잘못된 레코드 격리 (quarantine 패턴)

```python
# Silver 정제 예시
df_cleaned = (spark.readStream.table("catalog.bronze.raw_orders")
    .filter(col("order_id").isNotNull())
    .withColumn("order_date", to_date(col("order_date_str"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("decimal(18,2)"))
    .dropDuplicates(["order_id"])
)
```

#### Gold (비즈니스 집계)
- 부서/도메인별 KPI 테이블, 집계 뷰
- BI 도구(Tableau, Power BI)에서 직접 조회
- 비정규화 허용 (성능 > 정규화)

```python
# Gold 집계 예시
df_gold = (spark.read.table("catalog.silver.orders")
    .groupBy("region", "order_date")
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
)
df_gold.write.mode("overwrite").saveAsTable("catalog.gold.daily_sales_summary")
```

---

## 3. Delta Lake가 Lakehouse를 가능하게 하는 이유

Delta Lake = **Parquet 파일 + 트랜잭션 로그(`_delta_log/`)**

| 특성 | 설명 |
|------|------|
| **ACID 트랜잭션** | 여러 작업이 동시에 같은 테이블에 쓸 수 있음 (Optimistic Concurrency) |
| **Schema Enforcement** | 테이블 스키마와 안 맞는 데이터 쓰기 거부 |
| **Schema Evolution** | `mergeSchema` 옵션으로 컬럼 추가 허용 |
| **Time Travel** | 과거 버전 데이터 조회/롤백 |
| **Unified Batch + Streaming** | 같은 테이블에 배치와 스트리밍 동시 가능 |

→ 상세: [03-delta-lake-operations.md](03-delta-lake-operations.md)

---

## 4. 아키텍처 다이어그램

→ Mermaid 다이어그램: [assets/architecture_diagram.md](../assets/architecture_diagram.md)

```
┌─────────────────────────────────────────────────────┐
│                    Data Sources                      │
│  (DB, API, Files, Streaming, IoT)                   │
└──────────────────────┬──────────────────────────────┘
                       │ Auto Loader / COPY INTO / CDC
                       ▼
┌─────────────────────────────────────────────────────┐
│  Bronze (Raw)        │ 원본 그대로, 메타 컬럼 추가    │
├──────────────────────┼──────────────────────────────┤
│  Silver (Cleaned)    │ 정제, 중복 제거, 타입 보정      │
├──────────────────────┼──────────────────────────────┤
│  Gold (Curated)      │ KPI 집계, 비즈니스 뷰          │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      SQL / BI      ML / DS     Streaming
     (Tableau)    (MLflow)     (Real-time)
```

---

## 5. 핵심 요약

- **Lakehouse = Lake의 저장 + Warehouse의 품질**, Delta Lake가 그 접착제
- **Medallion Architecture**는 강제가 아닌 권장 패턴이지만, 실무에서 거의 표준
- Bronze은 **보험**(원본 보존), Silver는 **품질**, Gold는 **속도**에 집중
- 이 구조를 이해해야 이후 모든 문서(Delta, Unity Catalog, Jobs 등)가 연결됨
