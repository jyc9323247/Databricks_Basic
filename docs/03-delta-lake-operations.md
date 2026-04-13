# 🔺 03. Delta Lake DML/DDL, 최적화, 시간여행

> Delta Lake = Parquet + 트랜잭션 로그. 이것 때문에 Lakehouse가 가능합니다.

---

## 1. Delta Lake 핵심 특성 5가지

| 특성 | 설명 | 왜 중요한가 |
|------|------|-------------|
| **ACID 트랜잭션** | 읽기/쓰기 동시 안전 | 파이프라인 실패해도 데이터 깨지지 않음 |
| **Time Travel** | 과거 버전 조회/롤백 | 잘못된 UPDATE 되돌리기 가능 |
| **Schema Enforcement** | 스키마 불일치 쓰기 거부 | 잘못된 데이터 유입 차단 |
| **Schema Evolution** | 새 컬럼 자동 추가 | 소스 스키마 변경에 유연 대응 |
| **Unified Batch + Streaming** | 같은 테이블에 배치/스트리밍 공존 | 이중 파이프라인 불필요 |

---

## 2. 테이블 생성 (DDL)

### Managed Table (Unity Catalog 관리)

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.orders (
    order_id     BIGINT       COMMENT '주문 고유 ID',
    customer_id  BIGINT       COMMENT '고객 ID',
    order_date   DATE         COMMENT '주문일',
    amount       DECIMAL(18,2) COMMENT '주문 금액',
    status       STRING       COMMENT '주문 상태'
)
USING DELTA
COMMENT '주문 원장 테이블'
PARTITIONED BY (order_date)
TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

### External Table (외부 경로 지정)

```sql
CREATE TABLE IF NOT EXISTS catalog.schema.external_orders
USING DELTA
LOCATION 'abfss://container@storage.dfs.core.windows.net/data/orders';
```

### CTAS (CREATE TABLE AS SELECT)

```sql
CREATE OR REPLACE TABLE catalog.schema.active_orders AS
SELECT * FROM catalog.schema.orders WHERE status = 'active';
```

---

## 3. DML — INSERT, UPDATE, DELETE, MERGE

### INSERT

```sql
-- 단건
INSERT INTO catalog.schema.orders VALUES (1, 100, '2025-01-15', 50000.00, 'completed');

-- SELECT 결과 삽입
INSERT INTO catalog.schema.orders
SELECT * FROM catalog.staging.new_orders;
```

```python
# PySpark
df_new.write.mode("append").saveAsTable("catalog.schema.orders")
```

### UPDATE

```sql
UPDATE catalog.schema.orders
SET status = 'cancelled', amount = 0
WHERE order_id = 12345;
```

```python
# PySpark (DeltaTable API)
from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "catalog.schema.orders")
dt.update(
    condition="order_id = 12345",
    set={"status": "'cancelled'", "amount": "0"}
)
```

### DELETE

```sql
DELETE FROM catalog.schema.orders
WHERE status = 'test' AND order_date < '2024-01-01';
```

```python
dt.delete("status = 'test' AND order_date < '2024-01-01'")
```

### MERGE (Upsert) — 가장 중요

```sql
MERGE INTO catalog.schema.orders AS target
USING catalog.staging.new_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED AND source.status = 'cancelled' THEN
    DELETE

WHEN MATCHED THEN
    UPDATE SET
        target.amount = source.amount,
        target.status = source.status

WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_date, amount, status)
    VALUES (source.order_id, source.customer_id, source.order_date,
            source.amount, source.status);
```

```python
# PySpark MERGE
from delta.tables import DeltaTable

dt_target = DeltaTable.forName(spark, "catalog.schema.orders")
df_source = spark.read.table("catalog.staging.new_orders")

(dt_target.alias("t")
    .merge(df_source.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdate(set={
        "amount": "s.amount",
        "status": "s.status"
    })
    .whenNotMatchedInsertAll()
    .execute()
)
```

---

## 4. SCD Type 1 vs Type 2

### SCD Type 1 (최신 값으로 덮어쓰기)

```sql
MERGE INTO catalog.schema.customers AS t
USING catalog.staging.customer_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### SCD Type 2 (이력 보존)

→ 상세 코드: [code/patterns/scd_type2.py](../code/patterns/scd_type2.py)

```sql
-- 1) 기존 활성 레코드 종료
MERGE INTO catalog.schema.customers_hist AS t
USING catalog.staging.customer_updates AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.name != s.name OR t.address != s.address) THEN
    UPDATE SET t.is_current = false, t.end_date = current_date();

-- 2) 신규/변경 레코드 삽입
INSERT INTO catalog.schema.customers_hist
SELECT customer_id, name, address,
       current_date() AS start_date,
       NULL AS end_date,
       true AS is_current
FROM catalog.staging.customer_updates s
WHERE NOT EXISTS (
    SELECT 1 FROM catalog.schema.customers_hist t
    WHERE t.customer_id = s.customer_id
      AND t.is_current = true
      AND t.name = s.name
      AND t.address = s.address
);
```

---

## 5. Time Travel

### 버전별 조회

```sql
-- 특정 버전
SELECT * FROM catalog.schema.orders VERSION AS OF 5;

-- 특정 시점
SELECT * FROM catalog.schema.orders TIMESTAMP AS OF '2025-01-15 10:00:00';
```

```python
# PySpark
df_old = spark.read.format("delta").option("versionAsOf", 5).table("catalog.schema.orders")
df_old = spark.read.format("delta").option("timestampAsOf", "2025-01-15").table("catalog.schema.orders")
```

### 변경 이력 확인

```sql
DESCRIBE HISTORY catalog.schema.orders;
-- version, timestamp, operation, operationParameters 등 확인
```

### 롤백 (잘못된 변경 되돌리기)

```sql
-- 특정 버전으로 복원
RESTORE TABLE catalog.schema.orders TO VERSION AS OF 5;

-- 특정 시점으로 복원
RESTORE TABLE catalog.schema.orders TO TIMESTAMP AS OF '2025-01-15 10:00:00';
```

### 실무 활용 시나리오

| 시나리오 | 방법 |
|----------|------|
| 잘못된 DELETE 롤백 | `RESTORE TO VERSION AS OF N` |
| 특정 시점 데이터 비교 | 현재 vs `VERSION AS OF N` 비교 쿼리 |
| 데이터 감사 | `DESCRIBE HISTORY`로 누가 언제 변경했는지 확인 |

---

## 6. 테이블 최적화 3종

### OPTIMIZE (파일 컴팩션)

```sql
-- 기본: 작은 파일들을 합쳐서 최적 크기로
OPTIMIZE catalog.schema.orders;

-- 특정 파티션만
OPTIMIZE catalog.schema.orders WHERE order_date >= '2025-01-01';

-- Z-ORDER: 자주 필터링하는 컬럼 기준으로 데이터 재배치
OPTIMIZE catalog.schema.orders ZORDER BY (customer_id);

-- 복합 Z-ORDER (최대 4개 컬럼 권장)
OPTIMIZE catalog.schema.orders ZORDER BY (customer_id, order_date);
```

**언제 실행?**: 대량 데이터 적재 후, 또는 주기적 스케줄 (일 1회 등)

### VACUUM (오래된 파일 삭제)

```sql
-- 기본 7일(168시간) 이전 파일 삭제
VACUUM catalog.schema.orders;

-- 보존 기간 지정 (최소 7일 권장)
VACUUM catalog.schema.orders RETAIN 168 HOURS;

-- 삭제 대상 미리 확인 (DRY RUN)
VACUUM catalog.schema.orders DRY RUN;
```

**⚠️ 주의**: VACUUM 실행 후 해당 기간의 Time Travel 불가

### ANALYZE TABLE (통계 수집)

```sql
-- 전체 테이블 통계
ANALYZE TABLE catalog.schema.orders COMPUTE STATISTICS;

-- 특정 컬럼 통계 (옵티마이저가 더 좋은 실행 계획 수립)
ANALYZE TABLE catalog.schema.orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_date;
```

---

## 7. 스키마 관리

### Schema Enforcement (기본 동작)

```python
# 스키마 불일치 시 에러 발생
# AnalysisException: A schema mismatch detected when writing to the Delta table
df_wrong_schema.write.mode("append").saveAsTable("catalog.schema.orders")
```

### Schema Evolution (새 컬럼 허용)

```python
# 새 컬럼이 있는 DataFrame을 기존 테이블에 추가
df_with_new_col.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("catalog.schema.orders")
```

```sql
-- SQL에서 설정
ALTER TABLE catalog.schema.orders SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
```

### 실무에서 스키마 깨지는 시나리오

| 시나리오 | 증상 | 대응 |
|----------|------|------|
| 소스 컬럼 추가 | 쓰기 실패 | `mergeSchema = true` |
| 소스 컬럼 삭제 | NULL로 채워짐 | 파이프라인에서 명시적 처리 |
| 타입 변경 (int → string) | 쓰기 실패 | 캐스팅 로직 추가 또는 `overwriteSchema` |

---

## 8. Delta 로그 구조 (알아두면 좋은 것)

```
my_table/
├── _delta_log/
│   ├── 00000000000000000000.json   ← 각 트랜잭션 기록
│   ├── 00000000000000000001.json
│   ├── ...
│   └── 00000000000000000010.checkpoint.parquet  ← 10버전마다 체크포인트
├── part-00000-xxxx.parquet          ← 실제 데이터 파일들
├── part-00001-xxxx.parquet
└── ...
```

- JSON 로그: 각 트랜잭션에서 추가/삭제된 파일 목록
- Checkpoint: 10버전마다 전체 상태 스냅샷 (읽기 성능 최적화)
- Time Travel은 이 로그를 역추적해서 과거 버전을 재구성

---

## 9. 핵심 요약

- **MERGE**가 실무에서 가장 많이 쓰는 DML (Upsert 패턴)
- **Time Travel**로 실수를 되돌릴 수 있다는 것 = 안전망
- **OPTIMIZE + VACUUM** = Delta 테이블 유지보수의 양대 축
- Schema Enforcement가 기본 → `mergeSchema`로 진화 허용
- VACUUM 전에 보존 기간 확인 필수 (Time Travel 범위 제한)
