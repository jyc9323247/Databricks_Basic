# 🔐 04. Unity Catalog — 3-Level 네임스페이스 & 권한 관리

> 데이터 거버넌스의 출발점. "누가, 무엇을, 어디까지" 접근할 수 있는지 제어합니다.

---

## 1. 왜 Unity Catalog인가?

### 이전 방식 (Hive Metastore)

- 워크스페이스별 독립 메타스토어 → 교차 접근 불가
- 테이블 수준 권한만 가능 → 컬럼/행 단위 제어 불가
- 데이터 리니지 수동 관리 → 추적 어려움
- 스토리지 자격증명 Notebook에 하드코딩 → 보안 위험

### Unity Catalog가 해결하는 것

- **통합 거버넌스**: 모든 워크스페이스에서 하나의 메타스토어 공유
- **세밀한 접근 제어**: 테이블 + 컬럼 + 행 수준
- **자동 데이터 리니지**: 코드 실행 시 자동 추적
- **중앙 자격증명 관리**: Storage Credential, External Location

---

## 2. 3-Level 네임스페이스

```
catalog.schema.table
  │       │      │
  │       │      └── 테이블, 뷰, 함수
  │       └────────── 스키마 (= 데이터베이스)
  └────────────────── 카탈로그 (최상위 컨테이너)
```

### 계층 구조

```
Metastore (워크스페이스에 연결, 1개)
  └── Catalog (환경/도메인 단위)
        └── Schema (프로젝트/기능 단위)
              ├── Table (Managed / External)
              ├── View
              ├── Function (SQL UDF)
              └── Volume (비정형 파일 저장소)
```

### 네이밍 컨벤션 (권장)

| 레벨 | 패턴 | 예시 |
|------|------|------|
| Catalog | `{환경}_{도메인}` | `prod_sales`, `dev_analytics`, `sandbox` |
| Schema | `{레이어}` 또는 `{팀}` | `bronze`, `silver`, `gold`, `team_marketing` |
| Table | `{엔티티}_{설명}` | `orders_daily`, `customer_master` |

```sql
-- 실무 예시
SELECT * FROM prod_sales.gold.daily_revenue;
SELECT * FROM dev_analytics.silver.cleaned_events;
```

---

## 3. 오브젝트 생성

### Catalog 생성

```sql
CREATE CATALOG IF NOT EXISTS prod_sales
COMMENT '운영 환경 - 영업 도메인';

-- 소유자 변경
ALTER CATALOG prod_sales OWNER TO `sales_admin_group`;
```

### Schema 생성

```sql
CREATE SCHEMA IF NOT EXISTS prod_sales.bronze
COMMENT 'Raw 데이터 레이어';

CREATE SCHEMA IF NOT EXISTS prod_sales.silver
COMMENT '정제 데이터 레이어';

CREATE SCHEMA IF NOT EXISTS prod_sales.gold
COMMENT '비즈니스 집계 레이어';
```

### Volume 생성 (비정형 파일용)

```sql
-- Managed Volume (UC가 스토리지 관리)
CREATE VOLUME IF NOT EXISTS prod_sales.bronze.raw_files
COMMENT 'CSV/JSON 원본 파일 저장소';

-- 파일 접근
-- /Volumes/prod_sales/bronze/raw_files/2025/01/data.csv
```

---

## 4. 권한 관리 (GRANT / REVOKE)

### 권한 매트릭스

| 오브젝트 | 부여 가능한 권한 |
|----------|------------------|
| Catalog | `USE CATALOG`, `CREATE SCHEMA`, `ALL PRIVILEGES` |
| Schema | `USE SCHEMA`, `CREATE TABLE`, `CREATE VIEW`, `CREATE FUNCTION`, `ALL PRIVILEGES` |
| Table/View | `SELECT`, `MODIFY`, `ALL PRIVILEGES` |
| Function | `EXECUTE`, `ALL PRIVILEGES` |
| Volume | `READ VOLUME`, `WRITE VOLUME`, `ALL PRIVILEGES` |

### 실무 시나리오별 SQL

#### 분석가에게 읽기 전용

```sql
-- 카탈로그 사용 허가
GRANT USE CATALOG ON CATALOG prod_sales TO `analyst_group`;

-- Gold 스키마 사용 + 모든 테이블 SELECT
GRANT USE SCHEMA ON SCHEMA prod_sales.gold TO `analyst_group`;
GRANT SELECT ON SCHEMA prod_sales.gold TO `analyst_group`;
```

#### 엔지니어에게 Silver까지 쓰기 권한

```sql
GRANT USE CATALOG ON CATALOG prod_sales TO `engineer_group`;
GRANT USE SCHEMA ON SCHEMA prod_sales.bronze TO `engineer_group`;
GRANT USE SCHEMA ON SCHEMA prod_sales.silver TO `engineer_group`;
GRANT ALL PRIVILEGES ON SCHEMA prod_sales.bronze TO `engineer_group`;
GRANT ALL PRIVILEGES ON SCHEMA prod_sales.silver TO `engineer_group`;
```

#### 권한 회수

```sql
REVOKE SELECT ON SCHEMA prod_sales.gold FROM `former_analyst`;
```

#### 현재 권한 확인

```sql
SHOW GRANTS ON CATALOG prod_sales;
SHOW GRANTS ON SCHEMA prod_sales.gold;
SHOW GRANTS TO `analyst_group`;
```

---

## 5. 데이터 보호 3계층

| 계층 | 기능 | 보호 대상 | 적용 단위 |
|------|------|-----------|-----------|
| 접근 제어 | GRANT / REVOKE | 테이블/뷰 전체 | 사용자/그룹 |
| 행 필터링 | Row Filter | 특정 행 | 테이블 + 함수 |
| 컬럼 마스킹 | Column Mask | 특정 컬럼 값 | 테이블 + 함수 |

→ 심화: [04a-data-masking-filtering.md](04a-data-masking-filtering.md)
→ 감사: [04b-audit-and-lineage.md](04b-audit-and-lineage.md)

---

## 6. External Location & Storage Credential

### 개념

```
Storage Credential (Azure SPN, AWS IAM Role)
  └── External Location (특정 스토리지 경로에 매핑)
        └── External Table (해당 경로의 데이터를 테이블로 노출)
```

### 생성 예시 (Azure)

```sql
-- Storage Credential (관리자)
CREATE STORAGE CREDENTIAL IF NOT EXISTS adls_credential
WITH (AZURE_MANAGED_IDENTITY = '<managed-identity-id>');

-- External Location
CREATE EXTERNAL LOCATION IF NOT EXISTS raw_data_location
URL 'abfss://raw@storageaccount.dfs.core.windows.net/data/'
WITH (STORAGE CREDENTIAL adls_credential);

-- External Table
CREATE TABLE IF NOT EXISTS catalog.schema.ext_orders
USING DELTA
LOCATION 'abfss://raw@storageaccount.dfs.core.windows.net/data/orders/';
```

---

## 7. 마이그레이션 체크리스트 (Hive Metastore → Unity Catalog)

> ⚠️ 확실하지 않음: 마이그레이션 도구와 절차는 Databricks 버전에 따라 변경될 수 있습니다. 최신 공식 문서를 반드시 확인하세요.

- [ ] Metastore 생성 및 워크스페이스 연결
- [ ] Storage Credential, External Location 설정
- [ ] `SYNC` 명령 또는 마이그레이션 도우미로 테이블 마이그레이션
- [ ] 기존 `hive_metastore` 테이블에서 UC 테이블로 참조 변경
- [ ] 노트북/파이프라인에서 2-level 네이밍 → 3-level 네이밍 수정
- [ ] 기존 ACL → UC GRANT 재설정
- [ ] 테스트 쿼리로 데이터 정합성 확인

---

## 8. 핵심 요약

- `catalog.schema.table` 3단계 네이밍은 모든 쿼리의 기본
- **GRANT/REVOKE**로 그룹 기반 접근 제어 — 개인 단위보다 그룹 단위 권장
- Managed Table은 UC가 스토리지 관리, External Table은 직접 지정
- Storage Credential → External Location → External Table 순서로 설정
- Row Filter / Column Mask로 행·컬럼 단위 세밀한 제어 가능
