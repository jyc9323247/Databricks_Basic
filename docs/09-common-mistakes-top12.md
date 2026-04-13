# 🚫 09. 초보가 자주 하는 실수 TOP 12

> 실수에서 배우는 것이 가장 빠른 학습입니다. 미리 알면 더 빠르고요.

---

## 1. All-purpose 클러스터로 프로덕션 Job 돌리기

**증상**: DBU 청구서가 예상의 2배

**원인**: All-purpose Cluster는 DBU 단가가 Job Cluster 대비 1.5~2배

**해결**:
- Job 설정에서 "Job Cluster"로 변경
- All-purpose는 개발/디버깅 전용으로 한정

**비용 비교 예시**:
| 방식 | 월 예상 비용 |
|------|-------------|
| All-purpose (상시) | 약 $3,000 |
| Job Cluster (하루 4시간) | 약 $800 |

---

## 2. `collect()` 대용량 데이터에 호출

**증상**: `java.lang.OutOfMemoryError: Java heap space`

**원인**: `collect()`는 전체 데이터를 Driver 메모리로 가져옴. 100만 행이면 Driver 폭발.

**해결**:
```python
# ❌ BAD
all_data = df.collect()

# ✅ GOOD: 소량 확인
df.show(20)
df.display()  # Databricks 전용
df.take(10)
df.limit(100).toPandas()  # Pandas 변환도 소량만

# ✅ GOOD: 집계 후 가져오기
summary = df.groupBy("category").count().collect()
```

---

## 3. VACUUM 안 돌려서 스토리지 폭증

**증상**: 스토리지 비용이 계속 증가, 테이블 크기가 실제 데이터 대비 10배

**원인**: Delta Lake는 UPDATE/DELETE 시 기존 파일을 삭제하지 않음 (Time Travel 보존)

**해결**:
```sql
-- 주기적 VACUUM (최소 주 1회)
VACUUM catalog.schema.my_table RETAIN 168 HOURS;

-- OPTIMIZE도 함께 (파일 정리)
OPTIMIZE catalog.schema.my_table;
```

**예방**: Job으로 VACUUM + OPTIMIZE 스케줄링 (→ [delta_maintenance.sql](../code/sql/delta_maintenance.sql))

---

## 4. Small Files 문제 무시

**증상**: 쿼리가 갑자기 느려짐, `DESCRIBE DETAIL`에서 numFiles가 수만 개

**원인**: 빈번한 append, Streaming 마이크로배치, 잘못된 파티셔닝

**해결**:
```sql
ALTER TABLE catalog.schema.my_table SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);
OPTIMIZE catalog.schema.my_table;
```

---

## 5. Unity Catalog 없이 작업

**증상**: 나중에 UC 전환 시 모든 Notebook 경로 수정 + 권한 재설정

**원인**: `hive_metastore`(2-level)로 시작하면 `catalog.schema.table`(3-level)로 전환이 고통

**해결**: 처음부터 Unity Catalog 활성화 후 3-level 네이밍 사용

```python
# ❌ BAD
spark.read.table("my_database.my_table")

# ✅ GOOD
spark.read.table("catalog.schema.my_table")
```

---

## 6. 파티션 키 잘못 선택 (High Cardinality)

**증상**: 테이블에 파일이 수백만 개, 쓰기/읽기 모두 느림

**원인**: `user_id` (100만+)처럼 카디널리티 높은 컬럼으로 파티셔닝

**해결**:
```python
# ❌ BAD
df.write.partitionBy("user_id").saveAsTable(...)     # 100만 파티션

# ✅ GOOD: 카디널리티 낮은 컬럼
df.write.partitionBy("event_date").saveAsTable(...)   # 365개/년

# ✅ BETTER: 파티션 대신 Z-ORDER
OPTIMIZE my_table ZORDER BY (user_id);
```

**파티셔닝 가이드라인**: 파티션당 최소 1GB 이상 데이터가 있는 컬럼만 파티셔닝

---

## 7. Notebook에 하드코딩 (경로, 비밀번호)

**증상**: 비밀번호 노출, 환경 전환 시 모든 Notebook 수정 필요

**원인**: 편의상 직접 문자열 입력

**해결**:
```python
# ❌ BAD
password = "my_secret_123"
path = "/mnt/production/data/"

# ✅ GOOD: Secrets
password = dbutils.secrets.get(scope="my_scope", key="db_password")

# ✅ GOOD: Widget 파라미터
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
base_path = f"/mnt/{env}/data/"
```

---

## 8. Streaming 체크포인트 삭제/이동

**증상**: 데이터 중복 적재 또는 유실

**원인**: 체크포인트 = 스트리밍 처리 진행 상태. 삭제하면 처음부터 재처리.

**해결**:
- 체크포인트 경로는 **고정** — 한 번 설정하면 변경하지 않음
- 스키마 변경이 필요한 경우 → 새 테이블 + 새 체크포인트로 마이그레이션
- 체크포인트 백업 → 장애 복구용

---

## 9. `cache()` 남발

**증상**: 클러스터 메모리 부족, 다른 Job 실행 불가

**원인**: `cache()` 호출만 하고 `unpersist()` 안 함. 불필요한 DataFrame도 캐시.

**해결**:
```python
# ✅ 캐시 기준: 3회 이상 재사용하는 경우만
df_lookup = spark.read.table("code_master").cache()
df_lookup.count()  # Action으로 캐시 실행

# ... 사용 ...

df_lookup.unpersist()  # 반드시 해제
```

---

## 10. 스키마 변경 무시하고 append

**증상**: `AnalysisException: A schema mismatch detected`

**원인**: 소스에 새 컬럼이 추가되었는데 Delta 테이블 스키마와 불일치

**해결**:
```python
# 방법 1: mergeSchema (새 컬럼 허용)
df.write.option("mergeSchema", "true").mode("append").saveAsTable(...)

# 방법 2: 명시적 스키마 캐스팅
df_casted = df.select([col(c).cast(target_schema[c]) for c in target_cols])
```

---

## 11. 마스킹/필터 없이 민감 데이터 테이블 공유

**증상**: 분석가에게 `GRANT SELECT` 줬더니 주민번호, 급여 등 전부 노출

**원인**: 접근 권한만 관리하고, 컬럼/행 수준 보호를 안 함

**해결**:
```sql
-- 1. PII 태그 부여
ALTER TABLE catalog.schema.employees SET TAGS ('contains_pii' = 'true');

-- 2. 마스킹 함수 생성 & 적용 (→ 04a 참조)
ALTER TABLE catalog.schema.employees ALTER COLUMN salary SET MASK catalog.schema.mask_salary;

-- 3. 행 필터 추가 (→ 04a 참조)
ALTER TABLE catalog.schema.employees SET ROW FILTER catalog.schema.dept_filter ON (dept_id);
```

**예방 체크리스트** (테이블 공유 전):
- [ ] PII 컬럼에 Column Mask 적용?
- [ ] 부서/지역별 Row Filter 필요?
- [ ] 감사 로그에서 접근 패턴 확인? (→ [04b](04b-audit-and-lineage.md))

---

## 12. Spark UI 안 보고 감으로 튜닝

**증상**: "느려요" → `repartition(1000)` 추가, 클러스터 사이즈 2배 증가, 그래도 느림

**원인**: 병목 지점을 모르고 조치하면 오히려 악화

**해결**:
1. Spark UI → Stages → Task Duration 히스토그램 먼저 확인
2. 성능 튜닝 체크리스트(→ [05a 섹션 6](05a-performance-tuning.md)) 순서대로 진단
3. 조치 전후 지표 비교 기록

**원칙**: "측정 없이 튜닝 없다"

---

## 실수 영향도 매트릭스

| # | 실수 | 영향 영역 | 심각도 |
|---|------|-----------|--------|
| 1 | All-purpose으로 프로덕션 | 💰 비용 | 🔴 높음 |
| 2 | collect() 남용 | 🔧 안정성 | 🔴 높음 |
| 3 | VACUUM 미실행 | 💰 비용 | 🟡 중간 |
| 4 | Small Files | ⚡ 성능 | 🟡 중간 |
| 5 | UC 미사용 | 🔧 운영 | 🟡 중간 |
| 6 | 파티션 키 오선택 | ⚡ 성능 | 🟡 중간 |
| 7 | 하드코딩 | 🔒 보안 | 🔴 높음 |
| 8 | 체크포인트 삭제 | 📊 데이터 | 🔴 높음 |
| 9 | cache() 남발 | 🔧 안정성 | 🟡 중간 |
| 10 | 스키마 불일치 | 🔧 안정성 | 🟡 중간 |
| 11 | 마스킹 미적용 | 🔒 보안 | 🔴 높음 |
| 12 | 감 튜닝 | ⚡ 성능 + 💰 비용 | 🟡 중간 |
