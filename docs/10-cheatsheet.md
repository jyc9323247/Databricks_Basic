# 📝 10. 한 장 요약 치트시트

> 이 페이지를 북마크하세요. 실무에서 가장 자주 찾아보는 명령어 모음입니다.

---

## PySpark 자주 쓰는 코드 30선

### 읽기/쓰기

```python
df = spark.read.table("catalog.schema.table")                           # 1. Delta 테이블 읽기
df = spark.read.format("csv").option("header","true").load(path)        # 2. CSV 읽기
df.write.mode("overwrite").saveAsTable("catalog.schema.table")          # 3. 테이블 저장
df.write.mode("append").option("mergeSchema","true").saveAsTable(t)     # 4. 스키마 진화 + 추가
df.coalesce(1).write.option("header","true").csv(path)                  # 5. 단일 CSV 내보내기
```

### 선택/필터/변환

```python
df.select("col1", "col2")                                              # 6. 컬럼 선택
df.filter(col("age") >= 18)                                            # 7. 필터
df.withColumn("new_col", col("a") + col("b"))                          # 8. 컬럼 추가
df.withColumnRenamed("old", "new")                                      # 9. 컬럼 이름 변경
df.drop("temp_col")                                                     # 10. 컬럼 삭제
df.withColumn("grp", when(col("v")>100,"high").otherwise("low"))       # 11. 조건부 값
df.fillna({"amount": 0, "status": "unknown"})                          # 12. NULL 채우기
df.dropDuplicates(["key_col"])                                          # 13. 중복 제거
df.orderBy(col("created_at").desc())                                    # 14. 정렬
```

### 집계/조인

```python
df.groupBy("dept").agg(count("*"), avg("salary"))                      # 15. 집계
df.groupBy("dept").pivot("quarter").sum("revenue")                     # 16. 피벗
a.join(b, "key", "inner")                                              # 17. Inner Join
a.join(b, "key", "left_anti")                                          # 18. Anti Join (NOT IN)
a.join(broadcast(b), "key")                                             # 19. Broadcast Join
```

### 윈도우/문자열/날짜

```python
w = Window.partitionBy("dept").orderBy(col("sal").desc())
df.withColumn("rank", row_number().over(w))                            # 20. 순위
df.withColumn("prev", lag("val",1).over(w))                            # 21. 이전 값
df.withColumn("domain", split(col("email"),"@")[1])                    # 22. 문자열 분리
df.withColumn("clean", regexp_replace(col("phone"),r"[^0-9]",""))      # 23. 정규식 치환
df.withColumn("dt", to_date(col("str"),"yyyy-MM-dd"))                  # 24. 문자→날짜
df.withColumn("diff", datediff(current_date(), col("dt")))             # 25. 날짜 차이
```

### 기타

```python
df.explain(True)                                                        # 26. 실행 계획 확인
df.cache(); df.count()                                                  # 27. 캐시 (Action 필수)
df.unpersist()                                                          # 28. 캐시 해제
df.repartition("key_col")                                               # 29. 리파티션 (Shuffle)
df.coalesce(10)                                                         # 30. 파티션 줄이기 (No Shuffle)
```

---

## Delta SQL 자주 쓰는 명령 15선

```sql
-- DDL
CREATE TABLE IF NOT EXISTS c.s.t (id BIGINT, name STRING) USING DELTA;  -- 1
CREATE OR REPLACE TABLE c.s.t AS SELECT * FROM source;                   -- 2 (CTAS)
ALTER TABLE c.s.t ADD COLUMNS (new_col STRING);                          -- 3

-- DML
INSERT INTO c.s.t SELECT * FROM staging;                                 -- 4
UPDATE c.s.t SET status='done' WHERE id=1;                               -- 5
DELETE FROM c.s.t WHERE status='test';                                    -- 6
MERGE INTO c.s.t AS t USING src AS s ON t.id=s.id                        -- 7
  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;

-- Time Travel
SELECT * FROM c.s.t VERSION AS OF 5;                                     -- 8
DESCRIBE HISTORY c.s.t;                                                  -- 9
RESTORE TABLE c.s.t TO VERSION AS OF 5;                                  -- 10

-- 최적화
OPTIMIZE c.s.t;                                                          -- 11
OPTIMIZE c.s.t ZORDER BY (col1);                                         -- 12
VACUUM c.s.t RETAIN 168 HOURS;                                           -- 13
ANALYZE TABLE c.s.t COMPUTE STATISTICS FOR ALL COLUMNS;                  -- 14

-- 정보
DESCRIBE DETAIL c.s.t;                                                   -- 15
```

---

## dbutils 주요 명령어

```python
# 파일 시스템
dbutils.fs.ls("/path/")                                    # 파일 목록
dbutils.fs.head("/path/file.csv", 1000)                    # 파일 앞부분 읽기
dbutils.fs.cp("/src", "/dst", recurse=True)                # 복사
dbutils.fs.rm("/path/", recurse=True)                      # 삭제

# Secrets
dbutils.secrets.list("my_scope")                           # 시크릿 목록
dbutils.secrets.get("my_scope", "my_key")                  # 시크릿 값 읽기

# Widgets (파라미터)
dbutils.widgets.text("env", "dev")                         # 텍스트 위젯
dbutils.widgets.dropdown("mode", "full", ["full","incr"])  # 드롭다운
env = dbutils.widgets.get("env")                           # 값 읽기
dbutils.widgets.removeAll()                                # 전체 제거

# Notebook
dbutils.notebook.run("/path/to/notebook", 300, {"param":"value"})  # 다른 Notebook 실행 (timeout 300초)

# Job Task Values (Task 간 데이터 전달)
dbutils.jobs.taskValues.set(key="row_count", value=12345)       # 값 설정
dbutils.jobs.taskValues.get(taskKey="task_a", key="row_count")  # 값 읽기
```

---

## 매직 커맨드

```
%python    # Python 셀
%sql       # SQL 셀
%scala     # Scala 셀
%md        # Markdown (문서화)
%run /path/to/notebook    # 다른 Notebook 실행 (같은 컨텍스트)
%pip install pandas==2.0  # 라이브러리 설치 (현재 클러스터)
%sh ls -la /tmp           # Shell 명령
```

---

## Unity Catalog 권한 Quick Reference

```sql
-- 카탈로그
GRANT USE CATALOG ON CATALOG my_catalog TO `group_name`;
GRANT CREATE SCHEMA ON CATALOG my_catalog TO `group_name`;

-- 스키마
GRANT USE SCHEMA ON SCHEMA my_catalog.my_schema TO `group_name`;
GRANT SELECT ON SCHEMA my_catalog.my_schema TO `group_name`;
GRANT CREATE TABLE ON SCHEMA my_catalog.my_schema TO `group_name`;

-- 테이블
GRANT SELECT ON TABLE my_catalog.my_schema.my_table TO `group_name`;
GRANT MODIFY ON TABLE my_catalog.my_schema.my_table TO `group_name`;

-- 확인
SHOW GRANTS ON CATALOG my_catalog;
SHOW GRANTS TO `group_name`;

-- 소유자 변경
ALTER CATALOG my_catalog OWNER TO `new_owner_group`;
```

---

## Spark Config 빈출 설정

```python
spark.conf.set("spark.sql.shuffle.partitions", "200")                  # Shuffle 파티션
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)      # Broadcast 100MB
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")   # 자동 컴팩션
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true") # 최적화 쓰기
spark.conf.get("spark.sql.adaptive.enabled")                           # AQE 확인
```
