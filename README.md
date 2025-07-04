# Flink Online Feature Pipeline

## 개요
- Source : Kafka
- Processing : SlidingWindow, SessionWindow 활용
- Sink : Redis
## 시스템 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │ -> │     Kafka       │ -> │  Flink Stream   │ -> │  Redis Store    │
│  (E-commerce    │    │  (Event Queue)  │    │   Processing    │    │ (Feature Store) │
│    Events)      │    │                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │   Monitoring    │
                                               │  (Kafka UI +    │
                                               │  Flink WebUI)   │
                                               └─────────────────┘
```

## Features

### User Feaeture (1시간 윈도우)
Redis Key: `user_features:{user_id}`

| 필드명 | 타입 | 설명 | TTL |
|--------|------|------|-----|
| user_id | String | 사용자 ID | 2시간 |
| view_count_1h | Integer | 1시간 내 상품 조회 수 | 2시간 |
| cart_count_1h | Integer | 1시간 내 장바구니 추가 수 | 2시간 |
| purchase_count_1h | Integer | 1시간 내 구매 수 | 2시간 |
| avg_viewed_price_1h | Double | 1시간 내 평균 조회 상품 가격 | 2시간 |

### Session Feature
Redis Key: `session_features:{user_id}:{timestamp}`

| 필드명 | 타입 | 설명 | TTL |
|--------|------|------|-----|
| user_id | String | 사용자 ID | 1시간 |
| event_count | Integer | 세션 내 총 이벤트 수 | 1시간 |
| duration_seconds | Long | 세션 지속 시간 (초) | 1시간 |

### Event Schema
```json
{
  "event_time": "2024-01-01T10:00:00",
  "event_type": "view|cart|purchase",
  "user_id": "111",
  "price": 29.99
}
```

### Watermark
- 10초 지연 허용
```java
.<EcommerceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
```
### Window
```java
// 1시간 슬라이딩 윈도우 (10분 간격)
SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10))

// 세션 윈도우 (10분 비활성 갭)
EventTimeSessionWindows.withGap(Time.minutes(10))
```

## 실행 방법

### 환경 구성
```bash
./start.sh
```
Kafka, Redis 환경 구성

### 테스트 데이터 생성
`producer/notebooks`의 노트북을 실행하여 가짜 사용자 이벤트 데이터를 Kafka에 전송

### 모니터링 및 확인
- **Kafka UI**: http://localhost:8080
- **Flink WebUI**: http://localhost:8081
- **Redis 데이터 확인**: `docker exec -it redis redis-cli keys "*"`