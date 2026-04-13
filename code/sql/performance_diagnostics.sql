-- ============================================================
-- 성능 진단 쿼리 모음
-- Spark UI 외에 SQL로 확인할 수 있는 진단 도구들
-- ============================================================

-- ============================================================
-- 1. 테이블 크기 & 파일 수 확인 (Small Files 진단)
-- ============================================================

-- 단일 테이블
DESCRIBE DETAIL catalog.schema.my_table;

-- 스키마 내 모든 테이블 크기 비교 (수동 실행)
-- 각 테이블에 대해 DESCRIBE DETAIL 실행 후 비교


-- ============================================================
-- 2. 파티션별 데이터 분포 확인 (Skew 진단)
-- ============================================================

-- 파티션별 행 수 분포
SELECT order_date, COUNT(*) AS row_count
FROM catalog.schema.orders
GROUP BY order_date
ORDER BY row_count DESC
LIMIT 20;

-- 조인 키별 분포 (Skew 확인)
SELECT customer_id, COUNT(*) AS cnt
FROM catalog.schema.orders
GROUP BY customer_id
ORDER BY cnt DESC
LIMIT 20;


-- ============================================================
-- 3. 최근 작업 이력 확인
-- ============================================================

-- 최근 변경 이력 (OPTIMIZE, VACUUM, MERGE 등)
DESCRIBE HISTORY catalog.schema.orders LIMIT 30;


-- ============================================================
-- 4. 통계 정보 확인
-- ============================================================

-- 테이블 통계 수집 (옵티마이저 성능 개선)
ANALYZE TABLE catalog.schema.orders COMPUTE STATISTICS FOR ALL COLUMNS;

-- 수집된 통계 확인
DESCRIBE TABLE EXTENDED catalog.schema.orders;


-- ============================================================
-- 5. OPTIMIZE 효과 확인
-- ============================================================

-- OPTIMIZE 전: 파일 수 확인
-- SELECT numFiles FROM (DESCRIBE DETAIL catalog.schema.my_table);

-- OPTIMIZE 실행
OPTIMIZE catalog.schema.my_table;

-- OPTIMIZE 후: 파일 수 재확인
-- SELECT numFiles FROM (DESCRIBE DETAIL catalog.schema.my_table);


-- ============================================================
-- 6. Delta Log 크기 확인 (로그 비대화 진단)
-- ============================================================

-- 이력 수가 너무 많으면 메타데이터 작업 느려짐
-- 기본 로그 보존: 30일
SELECT COUNT(*) AS history_count
FROM (DESCRIBE HISTORY catalog.schema.my_table);

-- 로그 보존 기간 변경
-- ALTER TABLE catalog.schema.my_table
-- SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 14 days');


-- ============================================================
-- 7. SQL Warehouse 쿼리 이력 분석
-- ============================================================

-- 느린 쿼리 TOP 20 (system 테이블 활용)
-- ⚠️ 확실하지 않음: 쿼리 이력 시스템 테이블의 정확한 경로와 컬럼은
-- Databricks 버전별로 다를 수 있습니다.

-- SELECT query_id, user_name, query_text,
--        duration / 1000 AS duration_sec,
--        rows_produced
-- FROM system.query.history
-- WHERE start_time >= CURRENT_DATE - INTERVAL 7 DAYS
-- ORDER BY duration DESC
-- LIMIT 20;
