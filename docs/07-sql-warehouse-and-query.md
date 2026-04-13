# 📊 07. SQL Warehouse, DBSQL, BI 연동

> 분석가와 BI 도구가 Lakehouse에 접근하는 관문입니다.

---

## 1. SQL Warehouse 종류

| 종류 | 특징 | 비용 | 용도 |
|------|------|------|------|
| **Classic** | 기존 방식, 클러스터 기반 | 중간 | 레거시 호환 |
| **Pro** | Query Profile, 예측 최적화 | 높음 | 고급 분석 |
| **Serverless** | 인프라 관리 불필요, 즉시 시작 | DBU 단가 높지만 유휴 비용 없음 | 권장 (가변 워크로드) |

> ⚠️ 확실하지 않음: Serverless SQL Warehouse의 가용 리전과 요금 체계는 클라우드(Azure/AWS/GCP)별로 다를 수 있습니다.

### 선택 기준

```
가변적 워크로드 + 빠른 시작 필요 → Serverless
예산 고정 + 상시 운영 → Pro
최소 비용 + 간헐적 사용 → Classic (Auto-stop 짧게)
```

---

## 2. Warehouse 사이징

| 사이즈 | 클러스터 크기 | 적합 워크로드 |
|--------|-------------|---------------|
| 2X-Small ~ Small | 소규모 | 애드혹 쿼리, 대시보드 소수 사용자 |
| Medium | 중규모 | 동시 5~10명 분석, 중간 규모 테이블 |
| Large ~ 2X-Large | 대규모 | 동시 10명+, 수십억 행 테이블 |

### Auto-scaling 설정

```
Min clusters: 1 (최소 — 비용 절약)
Max clusters: 3~5 (동시 사용자 수에 따라)
Auto-stop: 10분 (유휴 시 자동 중지)
```

---

## 3. BI 도구 연동

### Partner Connect (가장 쉬움)

```
Workspace → Partner Connect → Tableau / Power BI / Looker 선택
→ 자동으로 Service Principal + Warehouse 연결 설정
```

### JDBC/ODBC 연결 문자열

```
Host:     adb-xxxxxxxxxxxx.xx.azuredatabricks.net
Port:     443
HTTP Path: /sql/1.0/warehouses/xxxxxxxxxx
Auth:     Personal Access Token 또는 OAuth
```

```
# Power BI 연결
1. Get Data → Azure Databricks
2. Server hostname 입력
3. HTTP Path 입력
4. 인증: Personal Access Token
```

---

## 4. DBSQL 대시보드

### 쿼리 → 시각화 → 대시보드 흐름

```
1. SQL Editor에서 쿼리 작성
2. 결과에서 "Visualization" 추가 (Bar, Line, Pie 등)
3. Dashboard에 시각화 위젯 배치
4. 스케줄 설정: 자동 갱신 (매시간, 매일 등)
5. 알림: 특정 조건 달성 시 Slack/Email
```

### 대시보드 쿼리 예시

```sql
-- 일별 매출 추이
SELECT order_date, SUM(amount) AS daily_revenue
FROM prod_sales.gold.daily_sales
WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY order_date
ORDER BY order_date;

-- 지역별 매출 비중
SELECT region, SUM(amount) AS total,
       ROUND(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER (), 1) AS pct
FROM prod_sales.gold.daily_sales
WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY region
ORDER BY total DESC;
```

---

## 5. 쿼리 최적화

### Query Profile 읽는 법

```
쿼리 실행 → 결과 하단 "Query Profile" 클릭
→ 실행 계획 트리에서 가장 시간이 오래 걸린 노드 확인
```

| 확인 항목 | 의미 |
|-----------|------|
| **Scan** 노드의 rows read | 불필요한 전체 스캔 여부 |
| **Shuffle** 크기 | 과도한 데이터 이동 |
| **Join** 타입 | BroadcastHashJoin vs SortMergeJoin |
| **Filter** 위치 | Scan에 밀어넣기(pushdown) 여부 |

### 최적화 팁

```sql
-- ① 필요한 컬럼만 SELECT (SELECT * 지양)
SELECT customer_id, order_date, amount
FROM orders
WHERE order_date = '2025-01-15';

-- ② 파티션 프루닝 활용 (파티션 컬럼으로 필터)
SELECT * FROM orders WHERE event_date = '2025-01-15';  -- 파티션 프루닝 발생

-- ③ 통계 수집
ANALYZE TABLE catalog.schema.orders COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## 6. 비용 관리

### Auto-stop 설정 (필수)

- 유휴 Warehouse가 켜져 있으면 비용 낭비
- 10분 Auto-stop 권장

### Warehouse 사용량 모니터링

```sql
-- SQL Warehouse별 DBU 사용량
SELECT warehouse_id, usage_date,
       SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE usage_type = 'SQL_WAREHOUSE'
  AND usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY warehouse_id, usage_date
ORDER BY usage_date DESC;
```

---

## 7. 핵심 요약

- **Serverless SQL Warehouse**가 대부분의 상황에서 권장 (관리 불필요, 빠른 시작)
- Auto-stop 10분 + Auto-scaling으로 비용 최적화
- BI 연동은 Partner Connect가 가장 쉬움
- Query Profile로 느린 쿼리 원인 파악 → 컬럼 선택, 파티션 프루닝, 통계 수집
- DBSQL 대시보드로 스케줄 자동 갱신 + 알림 설정 가능
