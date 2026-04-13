-- ============================================================
-- Unity Catalog 초기 설정 스크립트
-- Catalog / Schema / 권한 구성
-- ============================================================

-- ============================================================
-- 1. Catalog 생성
-- ============================================================

CREATE CATALOG IF NOT EXISTS prod_sales
COMMENT '운영 환경 - 영업 도메인';

CREATE CATALOG IF NOT EXISTS dev_sales
COMMENT '개발 환경 - 영업 도메인';

CREATE CATALOG IF NOT EXISTS sandbox
COMMENT '샌드박스 - 실험/탐색용';


-- ============================================================
-- 2. Schema 생성 (Medallion Architecture)
-- ============================================================

-- 운영 환경
CREATE SCHEMA IF NOT EXISTS prod_sales.bronze COMMENT 'Raw 데이터 레이어';
CREATE SCHEMA IF NOT EXISTS prod_sales.silver COMMENT '정제 데이터 레이어';
CREATE SCHEMA IF NOT EXISTS prod_sales.gold   COMMENT '비즈니스 집계 레이어';
CREATE SCHEMA IF NOT EXISTS prod_sales.ops    COMMENT '운영/로그 테이블';

-- 개발 환경
CREATE SCHEMA IF NOT EXISTS dev_sales.bronze COMMENT 'Raw 데이터 (개발)';
CREATE SCHEMA IF NOT EXISTS dev_sales.silver COMMENT '정제 데이터 (개발)';
CREATE SCHEMA IF NOT EXISTS dev_sales.gold   COMMENT '비즈니스 집계 (개발)';


-- ============================================================
-- 3. 권한 설정 — 데이터 엔지니어 그룹
-- ============================================================

-- 운영 환경: Bronze~Silver 전체 권한
GRANT USE CATALOG ON CATALOG prod_sales TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA prod_sales.bronze TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA prod_sales.silver TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA prod_sales.ops TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod_sales.bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod_sales.silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod_sales.ops TO `data_engineers`;

-- Gold는 SELECT + MODIFY만
GRANT USE SCHEMA ON SCHEMA prod_sales.gold TO `data_engineers`;
GRANT SELECT ON SCHEMA prod_sales.gold TO `data_engineers`;
GRANT MODIFY ON SCHEMA prod_sales.gold TO `data_engineers`;

-- 개발 환경: 전체 권한
GRANT USE CATALOG ON CATALOG dev_sales TO `data_engineers`;
GRANT ALL PRIVILEGES ON CATALOG dev_sales TO `data_engineers`;


-- ============================================================
-- 4. 권한 설정 — 분석가 그룹
-- ============================================================

-- Gold 읽기 전용
GRANT USE CATALOG ON CATALOG prod_sales TO `analysts`;
GRANT USE SCHEMA ON SCHEMA prod_sales.gold TO `analysts`;
GRANT SELECT ON SCHEMA prod_sales.gold TO `analysts`;

-- 샌드박스 전체 권한
GRANT USE CATALOG ON CATALOG sandbox TO `analysts`;
GRANT ALL PRIVILEGES ON CATALOG sandbox TO `analysts`;


-- ============================================================
-- 5. 권한 설정 — 관리자 그룹
-- ============================================================

GRANT ALL PRIVILEGES ON CATALOG prod_sales TO `data_admins`;
GRANT ALL PRIVILEGES ON CATALOG dev_sales TO `data_admins`;
GRANT ALL PRIVILEGES ON CATALOG sandbox TO `data_admins`;


-- ============================================================
-- 6. 권한 확인
-- ============================================================

SHOW GRANTS ON CATALOG prod_sales;
SHOW GRANTS ON SCHEMA prod_sales.gold;
SHOW GRANTS TO `analysts`;
SHOW GRANTS TO `data_engineers`;
