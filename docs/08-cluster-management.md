# 🖥️ 08. 클러스터 정책, 사이징, 비용 최적화

> 클러스터 설정이 비용의 70%를 결정합니다. 제대로 알고 쓰면 절반으로 줄일 수 있습니다.

---

## 1. 클러스터 유형 의사결정 트리

```
무엇을 하려는가?
  ├── 대화형 개발/탐색 → All-purpose Cluster
  ├── 프로덕션 Job 실행 → Job Cluster ✅ (비용 최적)
  └── SQL 쿼리/BI 분석 → SQL Warehouse
```

| 항목 | All-purpose | Job Cluster | SQL Warehouse |
|------|------------|-------------|---------------|
| 용도 | 개발, 디버깅 | 프로덕션 파이프라인 | SQL 분석, BI |
| 수명 | 수동 관리 | Job과 함께 생성/삭제 | Auto-stop |
| 과금 | 유휴 시에도 과금 | 실행 시간만 과금 | 실행 시간만 |
| DBU 단가 | 높음 | 낮음 (~60~70%) | 중간 |
| 공유 | 여러 사용자 | Job 전용 | 여러 사용자 |

---

## 2. 클러스터 구성 요소

### Driver vs Worker

```
Driver (1대): 전체 Job 조율, 결과 수집
Worker (N대): 실제 데이터 처리 (Task 실행)
```

- Driver 메모리 부족 → `collect()` 또는 `toPandas()` 호출 시 OOM
- Worker 수 = 병렬 처리 능력

### Spot vs On-demand

| 항목 | On-demand | Spot |
|------|-----------|------|
| 안정성 | ✅ 중단 없음 | ⚠️ 클라우드가 회수 가능 |
| 비용 | 100% | 30~70% 저렴 |
| 권장 비율 | Driver + 일부 Worker | 나머지 Worker |

```
권장 설정:
- Driver: On-demand (절대 Spot 금지)
- Worker: Spot 70% + On-demand 30% (혼합)
```

### Auto-scaling

```
Min Workers: 2 (최소 병렬도)
Max Workers: 10 (최대 비용 한도)
Auto-termination: 10분 (개발), 5분 (프로덕션)
```

---

## 3. Databricks Runtime 선택

| Runtime | 용도 | 포함 라이브러리 |
|---------|------|----------------|
| **Standard** | 일반 ETL | Spark, Delta Lake |
| **ML** | 머신러닝 | + TensorFlow, PyTorch, scikit-learn, MLflow |
| **Photon** | 고속 SQL | + Photon C++ 엔진 (SQL 워크로드 최적화) |
| **GPU** | 딥러닝/AI | + GPU 드라이버 |

```
ETL/데이터 엔지니어링 → Standard 또는 Photon
ML 모델 학습 → ML Runtime
SQL 집중 워크로드 → Photon 권장
```

> ⚠️ 확실하지 않음: Photon 사용 가능한 인스턴스 타입과 SKU는 클라우드/리전별로 다를 수 있습니다.

---

## 4. 클러스터 정책 (Cluster Policy)

관리자가 사용자의 클러스터 설정을 제한하는 기능.

### 제한해야 하는 항목

| 항목 | 이유 | 권장 정책 |
|------|------|-----------|
| Max Workers | 비용 폭증 방지 | 최대 10~20 |
| Instance Type | 고사양 인스턴스 사용 방지 | 허용 목록 지정 |
| Auto-termination | 유휴 클러스터 방치 방지 | 최소 10분 강제 |
| Spot 비율 | Spot 미사용 방지 | 최소 50% Spot 강제 |
| Runtime 버전 | 너무 오래된 버전 사용 방지 | LTS만 허용 |

### 정책 JSON 예시

```json
{
  "autotermination_minutes": {
    "type": "range",
    "minValue": 5,
    "maxValue": 30,
    "defaultValue": 10
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 10,
    "defaultValue": 2
  },
  "spark_version": {
    "type": "regex",
    "pattern": "15\\.[0-9]+\\.x-scala.*",
    "defaultValue": "15.4.x-scala2.12"
  },
  "node_type_id": {
    "type": "allowlist",
    "values": [
      "Standard_DS3_v2",
      "Standard_DS4_v2",
      "Standard_DS5_v2"
    ]
  }
}
```

---

## 5. 비용 최적화 TOP 5

### ① Job Cluster 사용 (가장 큰 절감)

```
All-purpose Cluster의 DBU 단가 ≈ Job Cluster의 1.5~2배
프로덕션 Job을 All-purpose에서 돌리고 있다면 → 즉시 전환
```

### ② Spot 인스턴스 비율 높이기

```
Worker의 70~80%를 Spot으로 설정
단, 짧은 Job (10분 이하)에서 가장 효과적
장시간 Job은 중단 위험 고려
```

### ③ Auto-termination 설정

```
개발 클러스터: 10분
프로덕션은 Job Cluster라 자동 종료됨
SQL Warehouse: Auto-stop 10분
```

### ④ 클러스터 풀 (Cluster Pool)

```
미리 VM을 예약해두고, 클러스터 생성 시 풀에서 할당
→ 클러스터 시작 시간 단축 (5분 → 30초)
→ Spot 회수 시 빠른 교체
```

### ⑤ 불필요한 라이브러리 제거

```
Init Script에서 pip install 대량 설치 → 클러스터 시작 시간 증가
필요 없는 라이브러리 정리, Cluster Library로 관리
```

---

## 6. Init Script & 환경 변수

### Init Script (클러스터 시작 시 실행)

```bash
#!/bin/bash
# 예: 특정 라이브러리 설치
pip install great-expectations==0.18.0
pip install openpyxl==3.1.2
```

저장 위치: Workspace Files, Unity Catalog Volume, 또는 클라우드 스토리지

### 환경 변수

```
클러스터 설정 → Advanced → Spark Config:
spark.executor.memory 8g
spark.driver.memory 4g

Environment Variables:
ENV_TYPE=production
LOG_LEVEL=INFO
```

---

## 7. 모니터링

### 클러스터 메트릭 확인

```
Compute → 클러스터 선택 → Metrics 탭
- CPU 사용률, 메모리 사용률
- Shuffle Read/Write
- GC 시간
```

### Ganglia UI (상세 모니터링)

```
클러스터 → Metrics → Ganglia UI 링크
- CPU/메모리/네트워크 실시간 그래프
- Worker별 상세 지표
```

---

## 8. 핵심 요약

- **Job Cluster + Spot 인스턴스** = 비용 최적화의 두 기둥
- Auto-termination은 반드시 설정 (10분 권장)
- 클러스터 정책으로 팀원의 과도한 사양 선택 방지
- Photon Runtime은 SQL 집중 워크로드에 효과적
- Driver는 절대 Spot으로 설정하지 말 것
