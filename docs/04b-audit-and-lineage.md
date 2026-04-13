# 📋 04b. 감사 로그 & 데이터 리니지

> "누가, 언제, 무엇을 했는가?" — 데이터 거버넌스의 사후 추적 체계입니다.

---

## 1. 시스템 테이블 기반 감사 (Audit Logs)

### Unity Catalog 시스템 테이블 구조

> ⚠️ 확실하지 않음: 시스템 테이블 스키마와 사용 가능 여부는 Databricks 플랜(Premium/Enterprise)과 리전에 따라 다를 수 있습니다. 아래 컬럼명은 참고용이며, 실제 환경에서 `DESCRIBE` 명령으로 확인하세요.

| 시스템 테이블 | 용도 | 보존 기간 |
|--------------|------|-----------|
| `system.access.audit` | 모든 UC 오브젝트 접근/변경 이력 | 365일 |
| `system.access.table_lineage` | 테이블 간 읽기/쓰기 의존 관계 | 365일 |
| `system.access.column_lineage` | 컬럼 단위 데이터 흐름 추적 | 365일 |
| `system.billing.usage` | 클러스터/Warehouse DBU 사용량 | - |
| `system.compute.clusters` | 클러스터 생성/삭제/설정 변경 | - |

### 시스템 테이블 활성화

```sql
-- 시스템 스키마 확인
SHOW SCHEMAS IN system;

-- 감사 테이블 존재 확인
SHOW TABLES IN system.access;

-- 컬럼 구조 확인
DESCRIBE system.access.audit;
```

---

## 2. 실무 감사 쿼리

### 특정 테이블 접근 이력

```sql
SELECT event_time,
       user_identity.email AS user_email,
       action_name,
       request_params.full_name_arg AS table_name,
       source_ip_address
FROM system.access.audit
WHERE action_name IN ('getTable', 'commandSubmit')
  AND request_params.full_name_arg = 'prod_sales.gold.daily_revenue'
  AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

### 권한 변경 이력 추적

```sql
SELECT event_time,
       user_identity.email AS changed_by,
       action_name,
       request_params
FROM system.access.audit
WHERE action_name IN ('updatePermissions', 'grantPermission', 'revokePermission')
  AND event_date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY event_time DESC;
```

### 데이터 삭제/수정 작업 추적

```sql
SELECT event_time,
       user_identity.email AS user_email,
       action_name,
       request_params.full_name_arg AS target_table,
       request_params.commandText AS query_text
FROM system.access.audit
WHERE action_name = 'commandSubmit'
  AND (
      request_params.commandText LIKE '%DELETE%'
      OR request_params.commandText LIKE '%UPDATE%'
      OR request_params.commandText LIKE '%DROP%'
  )
  AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

### 실패한 접근 시도 (Permission Denied)

```sql
SELECT event_time,
       user_identity.email AS user_email,
       action_name,
       request_params.full_name_arg AS target,
       response.error_message
FROM system.access.audit
WHERE response.status_code >= 400
  AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

### 비정상 접근 탐지 (심야/주말)

```sql
SELECT user_identity.email AS user_email,
       COUNT(*) AS access_count,
       MIN(event_time) AS first_access,
       MAX(event_time) AS last_access
FROM system.access.audit
WHERE (HOUR(event_time) < 6 OR HOUR(event_time) > 22)
  AND event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY user_identity.email
ORDER BY access_count DESC;
```

---

## 3. 감사 대시보드 구성 권장 항목

DBSQL 대시보드로 아래 지표를 모니터링하면 거버넌스 수준이 올라갑니다.

| 위젯 | 쿼리 기반 | 갱신 주기 |
|------|-----------|-----------|
| 일별 테이블 접근 TOP 10 | 사용자 × 테이블 카운트 | 일 1회 |
| 권한 변경 타임라인 | GRANT/REVOKE 이력 | 실시간 |
| 비정상 접근 카운트 | 심야/주말 + Permission Denied | 일 1회 |
| 민감 테이블 접근 리포트 | PII 태그 테이블 접근 | 주 1회 |
| 비용 사용량 추이 | `system.billing.usage` | 일 1회 |

---

## 4. 데이터 리니지

### 자동 리니지 추적

- Unity Catalog가 **Notebook, Job, DLT** 실행 시 자동으로 테이블/컬럼 리니지 기록
- 별도 설정 불필요 — UC Managed 테이블이면 자동 수집
- **지원되지 않는 경우**: 외부 도구(Airflow 등)에서 직접 JDBC로 쓰는 경우

### 테이블 리니지 쿼리

```sql
-- 특정 테이블에 데이터를 쓰는 상위 소스 추적
SELECT source_table_full_name,
       target_table_full_name,
       entity_type,
       event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'prod_sales.gold.daily_revenue'
ORDER BY event_time DESC
LIMIT 20;

-- 특정 테이블이 영향을 주는 하위 테이블 (다운스트림)
SELECT target_table_full_name,
       entity_type,
       event_time
FROM system.access.table_lineage
WHERE source_table_full_name = 'prod_sales.silver.orders'
ORDER BY event_time DESC;
```

### 컬럼 리니지 쿼리

```sql
-- 특정 컬럼이 어디서 왔는지 (업스트림)
SELECT source_table_full_name,
       source_column_name,
       target_table_full_name,
       target_column_name
FROM system.access.column_lineage
WHERE target_table_full_name = 'prod_sales.gold.daily_revenue'
  AND target_column_name = 'total_revenue';
```

### UI에서 리니지 확인

1. **Catalog Explorer** → 테이블 선택
2. **Lineage 탭** 클릭
3. 그래프 형태로 업스트림/다운스트림 시각화
4. 노드 클릭 시 해당 Job/Notebook으로 이동 가능

### 리니지 활용 시나리오

| 시나리오 | 리니지 활용법 |
|----------|-------------|
| 소스 테이블 스키마 변경 | 다운스트림 테이블 영향 범위 파악 |
| Gold 집계 수치 이상 | 업스트림 추적하여 원인 테이블 특정 |
| 테이블 폐기 검토 | 다운스트림 참조 여부 확인 (없으면 안전 삭제) |
| 컴플라이언스 감사 | PII 데이터 흐름 전체 경로 추적 |

---

## 5. 비용 감사

```sql
-- 일별 DBU 사용량 (클러스터별)
SELECT usage_date,
       workspace_id,
       cluster_id,
       usage_type,
       SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY usage_date, workspace_id, cluster_id, usage_type
ORDER BY total_dbu DESC;

-- 사용자별 비용 (Job 기준)
SELECT usage_metadata.job_id,
       usage_metadata.job_name,
       SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND usage_metadata.job_id IS NOT NULL
GROUP BY usage_metadata.job_id, usage_metadata.job_name
ORDER BY total_dbu DESC
LIMIT 20;
```

---

## 6. 핵심 요약

- **시스템 테이블**(`system.access.*`)이 감사의 핵심 데이터 소스
- **권한 변경 + 데이터 변경 + 실패 접근** 3가지를 반드시 모니터링
- **리니지는 자동 수집** — UC 테이블이면 별도 설정 불필요
- 리니지로 **영향 범위 분석** (스키마 변경, 테이블 폐기, 장애 원인 추적)
- DBSQL 대시보드로 감사 지표 시각화하면 거버넌스 수준 향상
