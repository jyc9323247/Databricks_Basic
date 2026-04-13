-- ============================================================
-- 시스템 테이블 감사 쿼리 모음
-- 사전 조건: system.access 스키마 활성화 필요 (Premium+)
-- ============================================================

-- ⚠️ 확실하지 않음: 시스템 테이블 컬럼명은 Databricks 버전/리전에
-- 따라 다를 수 있습니다. DESCRIBE 명령으로 확인 후 사용하세요.
-- DESCRIBE system.access.audit;


-- ============================================================
-- 1. 특정 테이블 접근 이력
-- ============================================================

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


-- ============================================================
-- 2. 권한 변경 이력
-- ============================================================

SELECT event_time,
       user_identity.email AS changed_by,
       action_name,
       request_params
FROM system.access.audit
WHERE action_name IN ('updatePermissions', 'grantPermission', 'revokePermission')
  AND event_date >= CURRENT_DATE - INTERVAL 30 DAYS
ORDER BY event_time DESC;


-- ============================================================
-- 3. 데이터 변경 작업 추적 (DELETE, UPDATE, DROP)
-- ============================================================

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


-- ============================================================
-- 4. 실패한 접근 시도 (Permission Denied)
-- ============================================================

SELECT event_time,
       user_identity.email AS user_email,
       action_name,
       request_params.full_name_arg AS target,
       response.error_message
FROM system.access.audit
WHERE response.status_code >= 400
  AND event_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY event_time DESC;


-- ============================================================
-- 5. 비정상 접근 탐지 (심야/주말)
-- ============================================================

SELECT user_identity.email AS user_email,
       COUNT(*) AS access_count,
       MIN(event_time) AS first_access,
       MAX(event_time) AS last_access
FROM system.access.audit
WHERE (HOUR(event_time) < 6 OR HOUR(event_time) > 22)
  AND event_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY user_identity.email
ORDER BY access_count DESC;


-- ============================================================
-- 6. 테이블 리니지 (업스트림 소스 추적)
-- ============================================================

SELECT source_table_full_name,
       target_table_full_name,
       entity_type,
       event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'prod_sales.gold.daily_revenue'
ORDER BY event_time DESC
LIMIT 20;


-- ============================================================
-- 7. 컬럼 리니지 (특정 컬럼 출처 추적)
-- ============================================================

SELECT source_table_full_name,
       source_column_name,
       target_table_full_name,
       target_column_name
FROM system.access.column_lineage
WHERE target_table_full_name = 'prod_sales.gold.daily_revenue'
  AND target_column_name = 'total_revenue';


-- ============================================================
-- 8. 비용 감사 — 일별 DBU 사용량
-- ============================================================

SELECT usage_date,
       usage_type,
       SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY usage_date, usage_type
ORDER BY usage_date DESC, total_dbu DESC;


-- ============================================================
-- 9. 비용 감사 — Job별 DBU 사용량 TOP 20
-- ============================================================

SELECT usage_metadata.job_id,
       usage_metadata.job_name,
       SUM(usage_quantity) AS total_dbu
FROM system.billing.usage
WHERE usage_date >= CURRENT_DATE - INTERVAL 30 DAYS
  AND usage_metadata.job_id IS NOT NULL
GROUP BY usage_metadata.job_id, usage_metadata.job_name
ORDER BY total_dbu DESC
LIMIT 20;
