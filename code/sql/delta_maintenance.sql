-- ============================================================
-- Delta 테이블 유지보수 스크립트
-- 주기: 일간 또는 주간 Job으로 스케줄링 권장
-- ============================================================

-- ============================================================
-- 1. OPTIMIZE (파일 컴팩션)
-- ============================================================

-- 기본 최적화
OPTIMIZE catalog.schema.orders;

-- 특정 파티션만 (최근 7일)
OPTIMIZE catalog.schema.orders
WHERE order_date >= CURRENT_DATE - INTERVAL 7 DAYS;

-- Z-ORDER (자주 필터링하는 컬럼 기준)
OPTIMIZE catalog.schema.orders
ZORDER BY (customer_id, order_date);


-- ============================================================
-- 2. VACUUM (오래된 파일 삭제)
-- ============================================================

-- 삭제 대상 미리 확인 (DRY RUN)
VACUUM catalog.schema.orders DRY RUN;

-- 7일(168시간) 이전 파일 삭제 (기본 권장)
VACUUM catalog.schema.orders RETAIN 168 HOURS;

-- ⚠️ 주의: VACUUM 실행 후 해당 기간의 Time Travel 불가


-- ============================================================
-- 3. ANALYZE TABLE (통계 수집)
-- ============================================================

-- 전체 통계
ANALYZE TABLE catalog.schema.orders COMPUTE STATISTICS;

-- 특정 컬럼 통계 (옵티마이저 성능 향상)
ANALYZE TABLE catalog.schema.orders
COMPUTE STATISTICS FOR COLUMNS customer_id, order_date, amount;


-- ============================================================
-- 4. 테이블 상태 확인
-- ============================================================

-- 파일 수, 크기, 파티션 정보
DESCRIBE DETAIL catalog.schema.orders;

-- 변경 이력 (최근 20건)
DESCRIBE HISTORY catalog.schema.orders LIMIT 20;

-- 테이블 속성 확인
SHOW TBLPROPERTIES catalog.schema.orders;


-- ============================================================
-- 5. 자동 최적화 설정 (테이블별)
-- ============================================================

ALTER TABLE catalog.schema.orders SET TBLPROPERTIES (
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
