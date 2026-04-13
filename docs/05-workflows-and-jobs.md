# ⚙️ 05. Jobs, Workflows, Task 오케스트레이션

> 파이프라인을 스케줄 걸고, 모니터링하고, 실패 시 대응하는 운영의 핵심입니다.

---

## 1. 핵심 개념 정리

| 개념 | 설명 |
|------|------|
| **Job** | 하나의 워크플로 단위 (= 하나의 파이프라인) |
| **Task** | Job 안의 개별 실행 단위 (Notebook, SQL, Python, DLT 등) |
| **Run** | Job의 한 번 실행 인스턴스 |
| **Workflow** | Job + 스케줄 + 알림 + 재시도를 포함한 전체 운영 설정 |

---

## 2. Task 유형

| 타입 | 용도 | 예시 |
|------|------|------|
| Notebook | 가장 범용, ETL/분석 | Bronze → Silver 변환 |
| Python Script | 외부 스크립트 실행 | 데이터 수집 스크립트 |
| SQL | 단일 SQL 실행 | OPTIMIZE, 집계 테이블 갱신 |
| dbt | dbt Core 작업 | 모델 빌드 |
| Delta Live Tables (DLT) | 선언적 파이프라인 | 실시간 스트리밍 파이프라인 |
| JAR | Java/Scala 작업 | 레거시 Spark 앱 |

---

## 3. Task 의존성 설정

### 선형 (순차 실행)

```
[Bronze 적재] → [Silver 정제] → [Gold 집계]
```

### 팬아웃 / 팬인 (병렬 + 합류)

```
[Bronze 적재] → [Silver 주문] ──┐
                                ├── [Gold 통합 집계]
[Bronze 적재] → [Silver 결제] ──┘
```

### 조건부 실행 (if/else)

```
[데이터 수집] → [품질 검증]
                  ├── (성공) → [Silver 적재]
                  └── (실패) → [알림 발송]
```

- 조건: `If/else condition` 또는 `Run if` 설정
- Task 결과 코드 기반: 성공(`SUCCESS`), 실패(`FAILED`), 조건(`ALL_DONE`, `AT_LEAST_ONE_SUCCESS`)

### For Each (반복 실행)

```
[파일 목록 조회] → [For Each 파일] → [개별 처리]
```

- 입력: JSON 배열
- 각 요소마다 하위 Task를 병렬 실행

---

## 4. 파라미터 전달

### Job Parameters → Task Parameters

```python
# Job 레벨 파라미터 설정 (UI 또는 API)
# key: "target_date", value: "2025-01-15"

# Notebook에서 받기 (방법 1: dbutils.widgets)
dbutils.widgets.text("target_date", "")
target_date = dbutils.widgets.get("target_date")

# Notebook에서 받기 (방법 2: Job 파라미터 직접 참조)
# Task 설정에서 Base parameters에 key-value 지정
```

### Task 간 값 전달 (Task Values)

```python
# Task A에서 값 설정
dbutils.jobs.taskValues.set(key="row_count", value=12345)

# Task B에서 값 읽기 (Task A 의존성 필요)
count = dbutils.jobs.taskValues.get(taskKey="task_a", key="row_count")
```

### 동적 파라미터 (Built-in References)

```
{{job.id}}                    # Job ID
{{job.run_id}}                # 현재 Run ID
{{job.start_time.iso_date}}   # 실행 시작일 (yyyy-MM-dd)
```

---

## 5. 스케줄링

### Cron 기반 스케줄

| 표현식 | 의미 |
|--------|------|
| `0 6 * * *` | 매일 오전 6시 |
| `0 */2 * * *` | 2시간마다 |
| `0 9 * * 1-5` | 평일 오전 9시 |
| `0 0 1 * *` | 매월 1일 자정 |

### 파일 기반 트리거

- 특정 스토리지 경로에 **파일이 도착하면** 자동 실행
- `File arrival` 트리거 설정: 모니터링 경로 + 대기 시간

### 연속 실행 (Continuous)

- 스트리밍 파이프라인용
- 이전 Run 종료 즉시 다음 Run 시작

---

## 6. 알림 설정

| 이벤트 | 알림 예시 |
|--------|-----------|
| Job 시작 | Slack/Email 알림 |
| Job 성공 | 생략 가능 (기본) |
| Job 실패 | ✅ 반드시 설정 |
| SLA 미달 (Duration Warning) | 예상 시간 초과 시 알림 |

### 설정 위치

```
Job 편집 → 오른쪽 패널 → "Email notifications" 또는 "Webhook notifications"
- On Start / On Success / On Failure / On Duration Warning
- Slack Webhook URL 또는 이메일 주소 입력
```

---

## 7. 재시도 정책

### Task 레벨 재시도

```
Task 설정 → "Retries"
- 재시도 횟수: 0~3 (권장: 1~2)
- 재시도 간격: 몇 초 대기 후 재실행
```

### 실패 시 부분 재실행 (Repair Run)

- 전체 Job을 다시 돌리지 않고, **실패한 Task부터** 재실행
- UI: Run 상세 → "Repair run" 버튼
- 이미 성공한 Task는 건너뜀 → 시간/비용 절약

---

## 8. Cluster 정책과 Job Cluster

### All-purpose vs Job Cluster

| 항목 | All-purpose Cluster | Job Cluster |
|------|-------------------|-------------|
| 용도 | 개발, 탐색, 디버깅 | 프로덕션 Job 실행 |
| 수명 | 수동 시작/종료 | Job 시작 시 생성, 종료 시 삭제 |
| 비용 | 높음 (유휴 시간 과금) | 낮음 (실행 시간만 과금) |
| 공유 | 여러 사용자 공유 | Job 전용 |
| **비용 차이** | **~2배 이상 비쌈** | **기준 가격** |

### 권장 사항

```
개발/디버깅: All-purpose Cluster (소규모, Auto-termination 10분)
프로덕션 Job: 반드시 Job Cluster 사용
     ↑ 이것만 지켜도 비용 30~50% 절감
```

---

## 9. 실무 운영 체크리스트

- [ ] 모든 프로덕션 Job은 Job Cluster로 설정했는가?
- [ ] 실패 알림(Email/Slack)이 설정되어 있는가?
- [ ] 재시도 횟수가 적절한가? (무한 재시도 방지)
- [ ] Duration Warning이 설정되어 있는가? (SLA 관리)
- [ ] 파라미터가 하드코딩 아닌 동적으로 전달되는가?
- [ ] 로그 확인 루틴이 있는가? (일간 Run 상태 점검)
- [ ] OPTIMIZE / VACUUM Job이 스케줄링되어 있는가?

---

## 10. 핵심 요약

- **Job Cluster**를 쓰는 것이 비용 최적화의 가장 큰 한 방
- Task 의존성으로 **팬아웃/팬인, 조건부, 반복** 모두 표현 가능
- `dbutils.jobs.taskValues`로 Task 간 데이터 전달
- **실패 알림은 필수**, Duration Warning은 권장
- Repair Run으로 실패 Task부터 재실행 (전체 재실행 불필요)
