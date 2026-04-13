# 🔒 04a. 데이터 마스킹 & 행 필터링 실무 가이드

> 테이블 전체 접근 권한은 주되, 민감 컬럼/행은 숨기는 기법입니다.

---

## 1. Column Masking (컬럼 마스킹)

### 개념

- 테이블 데이터 자체는 변경하지 않고, **읽는 시점에** 권한에 따라 값을 변환
- Unity Catalog에 등록된 SQL 함수를 마스킹 정책으로 연결

### 작동 방식

```
사용자 SELECT → UC 권한 체크 → 마스킹 함수 실행 → 원본 or 마스킹 값 반환
```

### 마스킹 함수 생성

```sql
-- 이메일 마스킹
CREATE OR REPLACE FUNCTION catalog.schema.mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('data_admin') THEN email
    ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1])
  END;

-- 전화번호 마스킹
CREATE OR REPLACE FUNCTION catalog.schema.mask_phone(phone STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('data_admin') THEN phone
    ELSE CONCAT('***-****-', RIGHT(phone, 4))
  END;

-- 주민번호 마스킹
CREATE OR REPLACE FUNCTION catalog.schema.mask_ssn(ssn STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('data_admin') THEN ssn
    ELSE CONCAT(LEFT(ssn, 6), '-*******')
  END;

-- 금액/급여 마스킹 (NULL 반환)
CREATE OR REPLACE FUNCTION catalog.schema.mask_salary(salary DECIMAL(18,2))
RETURNS DECIMAL(18,2)
RETURN
  CASE
    WHEN is_account_group_member('hr_admin') THEN salary
    ELSE NULL
  END;

-- 금액/급여 마스킹 (범위 치환)
CREATE OR REPLACE FUNCTION catalog.schema.mask_salary_range(salary DECIMAL(18,2))
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('hr_admin') THEN CAST(salary AS STRING)
    WHEN salary < 30000000 THEN '3천만 미만'
    WHEN salary < 50000000 THEN '3천만~5천만'
    WHEN salary < 70000000 THEN '5천만~7천만'
    ELSE '7천만 이상'
  END;
```

### 테이블에 마스킹 적용 / 해제

```sql
-- 적용
ALTER TABLE catalog.schema.customers
ALTER COLUMN email SET MASK catalog.schema.mask_email;

ALTER TABLE catalog.schema.customers
ALTER COLUMN phone SET MASK catalog.schema.mask_phone;

ALTER TABLE catalog.schema.employees
ALTER COLUMN salary SET MASK catalog.schema.mask_salary;

-- 해제
ALTER TABLE catalog.schema.customers
ALTER COLUMN email DROP MASK;
```

### 마스킹 패턴 레시피

| 데이터 유형 | 마스킹 방식 | 원본 → 마스킹 결과 |
|-------------|-------------|---------------------|
| 이메일 | 앞 2자 + `****` + 도메인 | `hong@gmail.com` → `ho****@gmail.com` |
| 전화번호 | 뒤 4자리만 표시 | `010-1234-5678` → `***-****-5678` |
| 주민번호 | 앞 6자리만 | `900101-1234567` → `900101-*******` |
| 카드번호 | 뒤 4자리만 | `1234-5678-9012-3456` → `****-****-****-3456` |
| 금액/급여 | NULL 또는 범위 치환 | `45000000` → `NULL` 또는 `3천만~5천만` |

### 주의사항

- 마스킹 함수는 **Unity Catalog에 등록된 SQL UDF만 가능** (PySpark UDF 불가)
- `is_account_group_member()` → 그룹 기반 분기, `current_user()` → 개인 기반 분기
- 마스킹된 컬럼에 JOIN/GROUP BY 시 **마스킹된 값** 기준으로 동작 → 분석 결과 왜곡 가능 ⚠️
- OWNER 권한을 가진 사용자도 마스킹 적용 대상 (예외 없음, 함수 내 분기 필요)

---

## 2. Row Filters (행 필터링)

### 개념

- 테이블에 **행 단위 접근 제어 함수**를 바인딩
- SELECT 시 함수의 RETURN 값이 `TRUE`인 행만 반환
- WHERE 절 자동 주입과 유사하지만, 사용자가 해제 불가

### 필터 함수 생성

```sql
-- 지역별 접근 제어
CREATE OR REPLACE FUNCTION catalog.schema.region_filter(region_code STRING)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_account_group_member('data_admin') THEN TRUE
    WHEN is_account_group_member('team_seoul') THEN region_code = 'SEL'
    WHEN is_account_group_member('team_busan') THEN region_code = 'BUS'
    WHEN is_account_group_member('team_daegu') THEN region_code = 'DGU'
    ELSE FALSE
  END;

-- 부서별 접근 제어 (자기 부서만)
CREATE OR REPLACE FUNCTION catalog.schema.dept_filter(dept_id INT)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_account_group_member('hr_admin') THEN TRUE
    WHEN is_account_group_member('dept_sales') THEN dept_id = 100
    WHEN is_account_group_member('dept_marketing') THEN dept_id = 200
    ELSE FALSE
  END;
```

### 테이블에 필터 적용 / 해제

```sql
-- 적용
ALTER TABLE catalog.schema.sales
SET ROW FILTER catalog.schema.region_filter ON (region_code);

-- 해제
ALTER TABLE catalog.schema.sales DROP ROW FILTER;
```

### Row Filter + Column Mask 동시 적용

```sql
-- 한 테이블에 둘 다 가능
ALTER TABLE catalog.schema.employees
SET ROW FILTER catalog.schema.dept_filter ON (dept_id);

ALTER TABLE catalog.schema.employees
ALTER COLUMN salary SET MASK catalog.schema.mask_salary;

-- 결과: 자기 부서 직원만 보이고, 급여는 마스킹됨
```

### Row Filter vs Dynamic View 비교

| 항목 | Row Filter | Dynamic View |
|------|-----------|--------------|
| 적용 대상 | 테이블 직접 | 별도 View 생성 필요 |
| 유지보수 | 함수만 수정 | View 재생성 필요 |
| 사용자 경험 | 원래 테이블명 그대로 사용 | View명 별도 안내 필요 |
| 성능 | 엔진 내부 최적화 | View 쿼리 오버헤드 가능 |
| UC 거버넌스 연동 | ✅ 네이티브 | ⚠️ 수동 관리 |
| 복잡한 로직 | SQL UDF로 가능 | 유연한 쿼리 가능 |

### 주의사항

- 필터 함수에서 다른 테이블 참조 시 해당 테이블 접근 권한도 필요
- `COUNT(*)` 결과도 필터 적용 → 사용자마다 건수가 다르게 보임 (정상 동작)
- `EXPLAIN`으로 필터 적용 여부 확인 가능

---

## 3. 실무 거버넌스 설계 예시

### 시나리오: 인사 테이블 보호

```
employees 테이블
├── emp_id (전체 공개)
├── name (전체 공개)
├── dept_id → Row Filter: 자기 부서만
├── email → Column Mask: 앞 2자 + ****
├── salary → Column Mask: HR만 원본, 나머지 NULL
└── ssn → Column Mask: HR만 원본, 나머지 앞 6자리만
```

```sql
-- 1. 마스킹 함수 생성 (위 섹션 참조)
-- 2. 행 필터 함수 생성 (위 섹션 참조)
-- 3. 적용
ALTER TABLE catalog.hr.employees
SET ROW FILTER catalog.hr.dept_filter ON (dept_id);

ALTER TABLE catalog.hr.employees ALTER COLUMN email SET MASK catalog.hr.mask_email;
ALTER TABLE catalog.hr.employees ALTER COLUMN salary SET MASK catalog.hr.mask_salary;
ALTER TABLE catalog.hr.employees ALTER COLUMN ssn SET MASK catalog.hr.mask_ssn;

-- 4. 확인
SELECT * FROM catalog.hr.employees;  -- 자기 부서만, 민감 컬럼 마스킹됨
```

---

## 4. 태그를 활용한 PII 관리

```sql
-- 테이블에 PII 태그 부여
ALTER TABLE catalog.schema.customers SET TAGS ('contains_pii' = 'true');

-- 컬럼에 분류 태그
ALTER TABLE catalog.schema.customers ALTER COLUMN email SET TAGS ('pii_type' = 'email');
ALTER TABLE catalog.schema.customers ALTER COLUMN phone SET TAGS ('pii_type' = 'phone');
ALTER TABLE catalog.schema.customers ALTER COLUMN ssn SET TAGS ('pii_type' = 'ssn');

-- PII 컬럼 검색 (Information Schema)
SELECT table_name, column_name, tag_name, tag_value
FROM system.information_schema.column_tags
WHERE tag_name = 'pii_type';
```

---

## 5. 핵심 요약

- **Column Mask**: 컬럼 값을 그룹/사용자에 따라 변환 (SQL UDF로 구현)
- **Row Filter**: 행 자체를 그룹/사용자에 따라 필터링 (SQL UDF로 구현)
- 두 기능 **동시 적용 가능** → 행 + 컬럼 조합 보호
- `is_account_group_member()`가 핵심 함수 — 그룹 기반 설계 권장
- 마스킹/필터 적용 테이블에서 JOIN, GROUP BY 시 결과 왜곡 주의
