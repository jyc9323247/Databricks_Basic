-- ============================================================
-- 행 필터 정책 예시
-- ============================================================

-- ============================================================
-- 1. 필터 함수 생성
-- ============================================================

-- 지역별 접근 제어
CREATE OR REPLACE FUNCTION catalog.schema.region_filter(region_code STRING)
RETURNS BOOLEAN
RETURN CASE
    WHEN is_account_group_member('data_admin') THEN TRUE
    WHEN is_account_group_member('team_seoul') THEN region_code = 'SEL'
    WHEN is_account_group_member('team_busan') THEN region_code = 'BUS'
    WHEN is_account_group_member('team_daegu') THEN region_code = 'DGU'
    WHEN is_account_group_member('team_gwangju') THEN region_code = 'GWJ'
    ELSE FALSE
END;

-- 부서별 접근 제어
CREATE OR REPLACE FUNCTION catalog.schema.dept_filter(dept_id INT)
RETURNS BOOLEAN
RETURN CASE
    WHEN is_account_group_member('hr_admin') THEN TRUE
    WHEN is_account_group_member('dept_sales') THEN dept_id = 100
    WHEN is_account_group_member('dept_marketing') THEN dept_id = 200
    WHEN is_account_group_member('dept_engineering') THEN dept_id = 300
    ELSE FALSE
END;

-- 개인 데이터 접근 제어 (자기 데이터만)
CREATE OR REPLACE FUNCTION catalog.schema.self_only_filter(user_email STRING)
RETURNS BOOLEAN
RETURN CASE
    WHEN is_account_group_member('data_admin') THEN TRUE
    ELSE user_email = current_user()
END;


-- ============================================================
-- 2. 테이블에 필터 적용
-- ============================================================

ALTER TABLE catalog.schema.sales
SET ROW FILTER catalog.schema.region_filter ON (region_code);

ALTER TABLE catalog.schema.employees
SET ROW FILTER catalog.schema.dept_filter ON (dept_id);


-- ============================================================
-- 3. Row Filter + Column Mask 동시 적용
-- ============================================================

-- 자기 부서 직원만 보이고, 급여는 마스킹
ALTER TABLE catalog.schema.employees
SET ROW FILTER catalog.schema.dept_filter ON (dept_id);

ALTER TABLE catalog.schema.employees
ALTER COLUMN salary SET MASK catalog.schema.mask_salary;

ALTER TABLE catalog.schema.employees
ALTER COLUMN ssn SET MASK catalog.schema.mask_ssn;


-- ============================================================
-- 4. 필터 해제
-- ============================================================

-- ALTER TABLE catalog.schema.sales DROP ROW FILTER;


-- ============================================================
-- 5. 필터 적용 확인
-- ============================================================

-- team_seoul 그룹 사용자: region_code = 'SEL'인 행만 보임
SELECT region_code, COUNT(*) AS cnt
FROM catalog.schema.sales
GROUP BY region_code;
