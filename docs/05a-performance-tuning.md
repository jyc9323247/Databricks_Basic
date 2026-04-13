# ⚡ 05a. 주니어를 위한 성능 튜닝 가이드

> "느린 건 다 이유가 있다" — 원인의 80%는 5가지 패턴에 집중됩니다.

---

## 0. 튜닝 전에 알아야 할 멘탈 모델

### Spark 실행 구조 한 줄 요약

```
Driver → Job → Stage → Task (= 파티션 1개 처리 단위)
```

### 느려지는 3대 원인 (이것만 기억)

| 원인 | 비유 | 핵심 지표 |
|------|------|-----------|
| **Shuffle** | 모든 택배를 허브에 모았다가 재분배 | Shuffle Read/Write 크기 |
| **Skew** | 한 사람에게 택배 10만 개 몰림 | Task 간 처리 시간 편차 |
| **Spill** | 책상이 좁아서 바닥에 쌓음 | Spill (Memory) / Spill (Disk) |

### Spark UI 읽는 법 (주니어 최소 필수)

| 탭 | 확인할 것 |
|----|-----------|
| **Jobs** | 전체 Job 목록, 실패 여부 |
| **Stages** | Stage별 Task 수, Shuffle 크기, 시간 분포 |
| **SQL/DataFrame** | 물리 실행 계획, 각 노드별 소요 시간 |
| **Storage** | 캐시된 RDD/DataFrame 목록 |

**핵심 습관**: "가장 오래 걸린 Stage를 클릭 → Task 시간 분포 히스토그램 확인"

---

## 1. Shuffle 최소화

### 왜 느린가

- Shuffle = 네트워크를 통한 데이터 재분배 (디스크 I/O + 직렬화 + 네트워크)
- JOIN, GROUP BY, DISTINCT, REPARTITION 시 발생
- 실행 계획에서 `Exchange` 노드 = Shuffle

### 진단

```python
# 실행 계획에서 Exchange 노드 확인
df.explain(True)
# Exchange hashpartitioning(...) 가 보이면 = Shuffle 발생
```

### 조치 패턴

#### ① 작은 테이블 JOIN → Broadcast Join

```python
from pyspark.sql.functions import broadcast

# 기준: 작은 쪽이 ~100MB 이하
result = big_df.join(broadcast(small_df), "key")
```

```sql
-- SQL에서는 힌트 사용
SELECT /*+ BROADCAST(small_table) */ *
FROM big_table
JOIN small_table USING (key);
```

**Broadcast 임계값 설정**:

```python
# 기본값 10MB → 실무에서는 100~512MB까지 올리는 경우 많음
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)  # 100MB (바이트)
```

#### ② 불필요한 Shuffle 제거

```python
# ❌ BAD: repartition 후 바로 write (의미 없는 shuffle)
df.repartition(100).write.mode("overwrite").save(path)

# ✅ GOOD: coalesce로 파티션 줄이기 (shuffle 없음)
df.coalesce(10).write.mode("overwrite").save(path)
```

> `repartition` = Shuffle 발생 (파티션 늘리거나 재분배)
> `coalesce` = Shuffle 없음 (파티션 줄이기만, 늘리기 불가)

#### ③ 같은 키로 여러 번 연산 → repartition 한 번 후 재사용

```python
df_partitioned = df.repartition("customer_id")
agg1 = df_partitioned.groupBy("customer_id").agg(...)
agg2 = df_partitioned.groupBy("customer_id").agg(...)
```

---

## 2. Data Skew 해소

### 왜 느린가

- 특정 키에 데이터가 몰리면 → 그 키 담당 Task 1개가 혼자 10분, 나머지 99개는 10초
- 전체 Stage 완료 시간 = **가장 느린 Task 시간**

### 진단

```
Spark UI → Stages 탭 → Task Duration 확인
- Median: 5초, Max: 8분 → ⚠️ Skew 확정
```

```python
# 코드로 skew 확인: 키별 건수 분포
df.groupBy("join_key").count().orderBy(col("count").desc()).show(20)
# 상위 키의 건수가 나머지 대비 10배 이상이면 Skew
```

### 조치 패턴

#### ① AQE Skew Join (가장 쉬움, 권장)

```python
# Databricks에서 기본 ON이지만 확인
spark.conf.get("spark.sql.adaptive.enabled")                        # true
spark.conf.get("spark.sql.adaptive.skewJoin.enabled")                # true
spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionFactor")  # 기본 5
spark.conf.get("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")  # 기본 256MB
```

#### ② Skew Hint (특정 테이블/컬럼 지정)

```sql
SELECT /*+ SKEW('orders', 'customer_id') */ *
FROM orders
JOIN customers USING (customer_id);
```

#### ③ Salting (AQE로 해결 안 될 때 수동 기법)

```python
from pyspark.sql.functions import concat, lit, floor, rand, explode, array

SALT_BUCKETS = 10

# 큰 테이블: 솔트 키 추가
big_salted = big_df.withColumn(
    "salt", floor(rand() * SALT_BUCKETS).cast("int")
).withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)

# 작은 테이블: 솔트 키 전체 복제
small_salted = small_df.withColumn(
    "salt", explode(array([lit(i) for i in range(SALT_BUCKETS)]))
).withColumn(
    "salted_key", concat(col("join_key"), lit("_"), col("salt"))
)

# Salted Join → 결과에서 salt 컬럼 제거
result = big_salted.join(small_salted, "salted_key").drop("salt", "salted_key")
```

→ 전체 코드: [code/patterns/skew_handling.py](../code/patterns/skew_handling.py)

---

## 3. Small Files 문제

### 왜 느린가

- 파일 1개 = Task 1개 → 파일이 1만 개면 Task 오버헤드 + 메타데이터 부하
- Streaming, 빈번한 append, 잘못된 파티셔닝에서 주로 발생

### 진단

```sql
-- 테이블의 파일 수와 크기 확인
DESCRIBE DETAIL catalog.schema.my_table;
-- numFiles, sizeInBytes 확인
-- 목표: 파일당 128MB ~ 1GB
```

```python
files = dbutils.fs.ls("abfss://container@storage/path/to/table/")
print(f"파일 수: {len(files)}")
avg_mb = sum(f.size for f in files) / len(files) / 1024 / 1024
print(f"평균 크기: {avg_mb:.1f} MB")
```

### 조치 패턴

#### ① OPTIMIZE (수동 컴팩션)

```sql
OPTIMIZE catalog.schema.my_table;
OPTIMIZE catalog.schema.my_table WHERE event_date >= '2025-01-01';
OPTIMIZE catalog.schema.my_table ZORDER BY (customer_id, event_date);
```

#### ② Auto Compaction + Optimized Write (예방)

```sql
ALTER TABLE catalog.schema.my_table SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
```

#### ③ 파티셔닝 재설계

```python
# ❌ BAD: high-cardinality 파티션 → 파일 폭증
df.write.partitionBy("user_id").format("delta").save(path)  # 100만 user = 100만+ 파일

# ✅ GOOD: low-cardinality (날짜, 지역 등)
df.write.partitionBy("event_date").format("delta").save(path)

# ✅ BETTER: 파티션 대신 Z-ORDER 또는 Liquid Clustering
```

---

## 4. 캐싱 전략

### 언제 캐시하면 효과 있는가

| 상황 | 캐시 효과 | 권장 |
|------|-----------|------|
| 동일 DataFrame 3회 이상 재사용 | ✅ 높음 | `cache()` 또는 `persist()` |
| 한 번 쓰고 버리는 DataFrame | ❌ 낭비 | 캐시 하지 말 것 |
| 원본이 자주 바뀌는 테이블 | ⚠️ 위험 | 캐시 하지 말 것 (stale 데이터) |
| 작은 룩업 테이블 | ✅ 높음 | Broadcast가 더 나을 수 있음 |

### 실무 코드

```python
# 캐시
df_lookup = spark.read.table("catalog.schema.code_master").cache()
df_lookup.count()  # ← Action 호출해야 실제 캐시됨 (Lazy Evaluation)

# 사용 후 반드시 해제
df_lookup.unpersist()
```

### 주의사항

- `cache()` = `persist(StorageLevel.MEMORY_AND_DISK)`
- 캐시가 메모리 초과 → Disk Spill → 오히려 느려질 수 있음
- **Delta Cache** (디스크 캐시)는 클러스터 레벨 자동 관리 — 수동 캐시와 별개

---

## 5. 파티션 수 튜닝

### 기본 개념

- 파티션 수 = Task 수 = 병렬도
- 너무 적으면 → 일부 Worker 과부하, OOM 위험
- 너무 많으면 → Task 스케줄링 오버헤드, Small Files

### 권장 기준

| 상황 | 권장 파티션 수 |
|------|---------------|
| 일반 배치 처리 | 데이터 크기(GB) × 2~4 |
| Shuffle 후 | `spark.sql.shuffle.partitions` 조정 |
| 파일 쓰기 전 | 목표 파일 크기 128MB~1GB 기준 역산 |

### 실무 설정

```python
# AQE가 자동 조정 (권장 — 별도 설정 불필요)
spark.conf.get("spark.sql.adaptive.enabled")  # true

# 수동 설정이 필요한 경우
spark.conf.set("spark.sql.shuffle.partitions", "50")    # 소규모 데이터
spark.conf.set("spark.sql.shuffle.partitions", "500")   # 대규모 데이터

# AQE 자동 파티션 병합 (기본 ON)
spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled")  # true
```

---

## 6. 주니어가 바로 쓰는 성능 진단 체크리스트

### Step 1: "어디가 느린지" 찾기

```
Job 실행 → Spark UI → SQL 탭 → 가장 오래 걸린 노드 클릭
```

### Step 2: 원인 5가지 중 어디에 해당하는지 판별

| 증상 (Spark UI에서 보이는 것) | 원인 | 바로가기 |
|-------------------------------|------|----------|
| Exchange 노드 소요 시간 큼 | Shuffle | → 섹션 1 |
| Task Duration 편차 극심 (max >> median) | Skew | → 섹션 2 |
| numFiles 수천~수만 개 | Small Files | → 섹션 3 |
| 동일 Stage 반복 실행 | 캐시 미스 | → 섹션 4 |
| Task 수 대비 데이터 극소/극대 | 파티션 수 부적절 | → 섹션 5 |

### Step 3: 조치 후 반드시 비교

```
튜닝 전후 비교 프레임:
1. 실행 시간 기록
2. Shuffle Read/Write 크기 비교
3. Spill 발생 여부 확인
4. Task 시간 분포 비교 (median vs max)
```

**원칙: "측정 없이 튜닝 없다"**

---

## 7. 자주 쓰는 Spark Config 레퍼런스

| 설정 | 기본값 | 권장/용도 |
|------|--------|-----------|
| `spark.sql.adaptive.enabled` | true | AQE 활성화 (**끄지 말 것**) |
| `spark.sql.adaptive.skewJoin.enabled` | true | Skew 자동 처리 |
| `spark.sql.shuffle.partitions` | 200 | AQE가 자동 조정 |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | 실무에서 100~512MB |
| `spark.databricks.delta.autoCompact.enabled` | false | `true` 권장 |
| `spark.databricks.delta.optimizeWrite.enabled` | false | `true` 권장 |
| `spark.sql.files.maxPartitionBytes` | 128MB | 읽기 시 파티션 크기 |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | 소규모 파티션 자동 병합 |

> ⚠️ 확실하지 않음: `spark.sql.autoBroadcastJoinThreshold`의 단위 설정 방식은 런타임 버전마다 다를 수 있습니다. 바이트 단위 정수값(예: `104857600`)이 가장 안전합니다.

---

## 8. 핵심 요약

- **AQE는 기본 ON** — 대부분의 Shuffle/Skew/파티션 문제를 자동 처리
- Broadcast Join은 작은 테이블 JOIN의 특효약 (임계값 올려줄 것)
- Small Files는 `OPTIMIZE` + `autoCompact`로 관리
- **Spark UI를 먼저 보고** 원인을 특정한 후 조치할 것
- 감으로 `repartition(1000)` 넣지 말고, 데이터 크기 기반으로 계산
