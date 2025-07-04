# Flink Online Feature Pipeline

## 개요
- Source : Kafka
- Processing : SlidingWindow, SessionWindow 활용
- Sink : Redis

### Watermark
- 10초 지연 허용
```java

```

### 1시간 사용자 활동 피처
- SlidingWindow 사용, 윈도우 크기 1시간, 슬라이딩 간격 10분
```java
DataStream<UserFeature> userFeatures = eventStream
    .keyBy(event -> event.user_id)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
    .aggregate(new UserFeatureAggregator()).name("User Feature Aggregator");
```

### 세션 피처
- 세션 내 총 이벤트 수
- 세션 지속 시간
- 30분 비활성 시 세션 종료
```java
DataStream<SessionFeature> sessionFeatures = eventStream
    .keyBy(event -> event.user_id)
    .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .aggregate(new SessionAggregator()).name("Session Feature Aggregator");
```

## 실행 방법

### 환경 구성
```bash
./start.sh
```
Kafka, Redis 환경 구성

### 테스트 데이터 생성
`producer/notebooks`의 노트북을 실행하여 가짜 사용자 이벤트 데이터를 Kafka에 전송