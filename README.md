# 🧱 Databricks Core Guide — 실무 80%를 커버하는 핵심 20%

> 파레토 법칙 기반, 주니어 데이터 엔지니어를 위한 Databricks 실전 가이드

![Databricks](https://img.shields.io/badge/Databricks-Runtime_15.x-FF3621?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-00ADD8)
![Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)
![Language](https://img.shields.io/badge/language-Korean-blue)

---

## 🎯 누구를 위한 가이드인가?

- Databricks를 처음 접하는 데이터 엔지니어
- Spark / Delta Lake 기본은 아는데 실무 패턴이 부족한 주니어
- 온프레미스 DW에서 클라우드 Lakehouse로 전환 중인 팀
- Databricks Associate / Professional 자격증 준비자

---

## 📁 디렉토리 구조

```
databricks-core-guide/
├── README.md                              # 이 문서
├── docs/
│   ├── 01-lakehouse-architecture.md       # Lakehouse 아키텍처 & Medallion
│   ├── 02-spark-dataframe.md              # Spark DataFrame API 실무 패턴
│   ├── 03-delta-lake-operations.md        # Delta Lake DML/DDL, 최적화, 시간여행
│   ├── 04-unity-catalog.md                # Unity Catalog 3-level 네임스페이스 & 권한
│   ├── 04a-data-masking-filtering.md      # 컬럼 마스킹 & 행 필터링 심화
│   ├── 04b-audit-and-lineage.md           # 감사 로그, 시스템 테이블, 리니지
│   ├── 05-workflows-and-jobs.md           # Jobs, Workflows, Task 오케스트레이션
│   ├── 05a-performance-tuning.md          # 주니어를 위한 성능 튜닝 가이드
│   ├── 06-data-ingestion.md               # Auto Loader, COPY INTO, 증분 수집 패턴
│   ├── 07-sql-warehouse-and-query.md      # SQL Warehouse, DBSQL, BI 연동
│   ├── 08-cluster-management.md           # 클러스터 정책, 사이징, 비용 최적화
│   ├── 09-common-mistakes-top12.md        # 초보가 자주 하는 실수 TOP 12
│   └── 10-cheatsheet.md                   # 한 장 요약 치트시트
├── code/
│   ├── patterns/
│   │   ├── bronze_ingestion.py            # Bronze 레이어 Auto Loader 패턴
│   │   ├── silver_transform.py            # Silver 레이어 정제/변환 패턴
│   │   ├── gold_aggregation.py            # Gold 레이어 집계 패턴
│   │   ├── scd_type2.py                   # SCD Type 2 MERGE 패턴
│   │   ├── dedup_pattern.py               # 중복 제거 패턴
│   │   ├── schema_evolution.py            # 스키마 진화 처리 패턴
│   │   ├── skew_handling.py               # 데이터 Skew 해소 패턴
│   │   └── broadcast_join.py              # Broadcast Join 적용 패턴
│   ├── utilities/
│   │   ├── logging_utils.py               # 작업 로깅 유틸리티
│   │   ├── data_quality_checks.py         # DQ 체크 함수 모음
│   │   └── mount_storage.py               # 스토리지 마운트/연결 (ADLS, S3)
│   └── sql/
│       ├── delta_maintenance.sql          # OPTIMIZE, VACUUM, ANALYZE 스크립트
│       ├── unity_catalog_setup.sql        # 카탈로그/스키마/권한 초기 설정
│       ├── troubleshooting.sql            # 자주 쓰는 디버깅 쿼리
│       ├── column_masking.sql             # 마스킹 함수 & 정책 적용 예시
│       ├── row_filters.sql                # 행 필터 정책 예시
│       ├── audit_queries.sql              # 시스템 테이블 감사 쿼리 모음
│       └── performance_diagnostics.sql    # 성능 진단 쿼리
└── assets/
    └── architecture_diagram.md            # Mermaid 기반 아키텍처 다이어그램
```

---

## 📚 학습 로드맵 (권장 4주)

| 주차 | 주제 | 문서 | 코드 |
|------|------|------|------|
| **1주** | 아키텍처 이해 + Spark 기본 | [01](docs/01-lakehouse-architecture.md), [02](docs/02-spark-dataframe.md) | - |
| **2주** | Delta Lake + 데이터 수집 | [03](docs/03-delta-lake-operations.md), [06](docs/06-data-ingestion.md) | `bronze_ingestion.py`, `silver_transform.py` |
| **3주** | Unity Catalog + 거버넌스 + SQL Warehouse | [04](docs/04-unity-catalog.md), [04a](docs/04a-data-masking-filtering.md), [04b](docs/04b-audit-and-lineage.md), [07](docs/07-sql-warehouse-and-query.md) | `unity_catalog_setup.sql`, `column_masking.sql` |
| **4주** | Jobs/운영 + 성능 튜닝 + 클러스터 + 실수 방지 | [05](docs/05-workflows-and-jobs.md), [05a](docs/05a-performance-tuning.md), [08](docs/08-cluster-management.md), [09](docs/09-common-mistakes-top12.md) | `delta_maintenance.sql`, `performance_diagnostics.sql` |

---

## 🚀 빠른 시작

### 전제 조건

- Databricks Workspace (Azure / AWS / GCP)
- Databricks Runtime 13.3 LTS 이상 (15.x 권장)
- Unity Catalog 활성화된 환경

### 사용 방법

1. **순서대로 학습**: `01` → `10` 순서로 읽으면 자연스럽게 연결됩니다
2. **코드 실습**: `code/` 폴더의 패턴을 복사해서 Notebook에서 바로 실행
3. **운영 참고**: 장애 발생 시 `09-common-mistakes-top12.md`에서 증상 확인
4. **빠른 레퍼런스**: `10-cheatsheet.md`를 북마크해두고 수시로 참고

---

## ⚙️ 환경 정보

| 항목 | 권장 값 |
|------|---------|
| Databricks Runtime | 15.x LTS |
| Delta Lake | 3.x |
| Apache Spark | 3.5.x |
| Unity Catalog | 활성화 필수 |
| Cloud | Azure / AWS / GCP |

---

## 📜 License

MIT License — 자유롭게 사용, 수정, 배포 가능합니다.
