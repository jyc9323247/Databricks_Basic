-- ============================================================
-- 자주 쓰는 디버깅 쿼리 모음
-- ============================================================

-- ============================================================
-- 1. 테이블 정보 확인
-- ============================================================

-- 테이블 상세 (파일 수, 크기, 파티션)
DESCRIBE DETAIL catalog.schema.my_table;

-- 스키마 확인
DESCRIBE TABLE EXTENDED catalog.schema.my_table;

-- 테이블 속성
SHOW TBLPROPERTIES catalog.schema.my_table;

-- 변경 이력
DESCRIBE HISTORY catalog.schema.my_table LIMIT 20;


-- ============================================================
-- 2. 데이터 정합성 확인
-- ============================================================

-- 행 수 확인
SELECT COUNT(*) AS total_rows FROM catalog.schema.my_table;

-- NULL 비율 확인
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_name,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount
FROM catalog.schema.my_table;

-- 중복 키 확인
SELECT order_id, COUNT(*) AS cnt
FROM catalog.schema.orders
GROUP BY order_id
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 20;

-- 날짜별 건수 추이 (데이터 누락 탐지)
SELECT order_date, COUNT(*) AS cnt
FROM catalog.schema.orders
WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY order_date
ORDER BY order_date;


-- ============================================================
-- 3. 버전 비교 (Time Travel 활용)
-- ============================================================

-- 현재 vs 이전 버전 행 수 비교
SELECT 'current' AS version, COUNT(*) AS cnt FROM catalog.schema.my_table
UNION ALL
SELECT 'version_5', COUNT(*) FROM catalog.schema.my_table VERSION AS OF 5;

-- 특정 버전에서 삭제된 레코드 확인
SELECT * FROM catalog.schema.my_table VERSION AS OF 5
EXCEPT
SELECT * FROM catalog.schema.my_table;


-- ============================================================
-- 4. Small Files 진단
-- ============================================================

-- 파일 수와 평균 크기 확인
SELECT
    numFiles,
    ROUND(sizeInBytes / 1024 / 1024, 1) AS size_mb,
    ROUND(sizeInBytes / 1024 / 1024 / numFiles, 1) AS avg_file_mb
FROM (DESCRIBE DETAIL catalog.schema.my_table);


-- ============================================================
-- 5. 스키마 간 테이블 목록
-- ============================================================

-- 특정 스키마의 모든 테이블
SHOW TABLES IN catalog.schema;

-- 카탈로그의 모든 스키마
SHOW SCHEMAS IN catalog;
