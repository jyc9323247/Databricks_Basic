-- ============================================================
-- 컬럼 마스킹 함수 & 정책 적용 예시
-- ============================================================

-- ============================================================
-- 1. 마스킹 함수 생성
-- ============================================================

-- 이메일 마스킹 (앞 2자 + **** + 도메인)
CREATE OR REPLACE FUNCTION catalog.schema.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('data_admin') THEN email
    ELSE CONCAT(LEFT(email, 2), '****@', SPLIT(email, '@')[1])
END;

-- 전화번호 마스킹 (뒤 4자리만 표시)
CREATE OR REPLACE FUNCTION catalog.schema.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('data_admin') THEN phone
    ELSE CONCAT('***-****-', RIGHT(phone, 4))
END;

-- 주민번호 마스킹 (앞 6자리만)
CREATE OR REPLACE FUNCTION catalog.schema.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('data_admin') THEN ssn
    ELSE CONCAT(LEFT(ssn, 6), '-*******')
END;

-- 급여 마스킹 (NULL 반환)
CREATE OR REPLACE FUNCTION catalog.schema.mask_salary(salary DECIMAL(18,2))
RETURNS DECIMAL(18,2)
RETURN CASE
    WHEN is_account_group_member('hr_admin') THEN salary
    ELSE NULL
END;

-- 급여 마스킹 (범위 치환)
CREATE OR REPLACE FUNCTION catalog.schema.mask_salary_range(salary DECIMAL(18,2))
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('hr_admin') THEN CAST(salary AS STRING)
    WHEN salary < 30000000 THEN '3천만 미만'
    WHEN salary < 50000000 THEN '3천만~5천만'
    WHEN salary < 70000000 THEN '5천만~7천만'
    ELSE '7천만 이상'
END;

-- 카드번호 마스킹 (뒤 4자리만)
CREATE OR REPLACE FUNCTION catalog.schema.mask_card(card_no STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('data_admin') THEN card_no
    ELSE CONCAT('****-****-****-', RIGHT(card_no, 4))
END;


-- ============================================================
-- 2. 테이블에 마스킹 적용
-- ============================================================

ALTER TABLE catalog.schema.customers ALTER COLUMN email    SET MASK catalog.schema.mask_email;
ALTER TABLE catalog.schema.customers ALTER COLUMN phone    SET MASK catalog.schema.mask_phone;
ALTER TABLE catalog.schema.customers ALTER COLUMN ssn      SET MASK catalog.schema.mask_ssn;
ALTER TABLE catalog.schema.employees ALTER COLUMN salary   SET MASK catalog.schema.mask_salary;


-- ============================================================
-- 3. 마스킹 해제
-- ============================================================

-- ALTER TABLE catalog.schema.customers ALTER COLUMN email DROP MASK;


-- ============================================================
-- 4. 마스킹 적용 확인
-- ============================================================

-- 관리자로 조회 → 원본 보임
-- 일반 사용자로 조회 → 마스킹 적용됨
SELECT email, phone, ssn FROM catalog.schema.customers LIMIT 5;
