package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class UserFeatureJob {

    // 이벤트 데이터 모델
    public static class EcommerceEvent {
        public String event_time;
        public String event_type;
        public String product_id;
        public String category_id;
        public String category_code;
        public String brand;
        public double price;
        public String user_id;
        public String user_session;

        public EcommerceEvent() {
        }

        public long getEventTimeMillis() {
            try {
                // KST 기준으로 파싱해서 UTC 밀리초로 변환
                LocalDateTime kstTime = LocalDateTime.ofInstant(
                    Instant.parse(event_time),
                    ZoneOffset.of("+09:00")
                );
                long millis = kstTime.toInstant(ZoneOffset.of("+09:00")).toEpochMilli();
                return millis;
            } catch (Exception e) {
                return System.currentTimeMillis();
            }
        }

        public int getHourOfDay() {
            try {
                // KST 기준으로 시간 추출
                return LocalDateTime.ofInstant(
                    Instant.parse(event_time),
                    ZoneOffset.of("+09:00")
                ).getHour();
            } catch (Exception e) {
                return LocalDateTime.now(ZoneOffset.of("+09:00")).getHour();
            }
        }
    }

    // 시간대 비율 피처만 포함한 모델
    public static class UserFeatureVector {
        public String user_id;
        public long timestamp;
        public Map<String, Double> time_slot_ratios;

        public UserFeatureVector() {
            this.time_slot_ratios = new HashMap<>();
        }

        @Override
        public String toString() {
            return String.format("UserFeature{user=%s, ratios=%s}",
                user_id, time_slot_ratios);
        }
    }

    // 윈도우 집계용 누적기
    public static class TimeSlotAccumulator {
        public String userId;
        public Integer[] hourlyCounts;

        public TimeSlotAccumulator() {
            this.hourlyCounts = new Integer[24];
            for (int i = 0; i < 24; i++) {
                this.hourlyCounts[i] = 0;
            }
        }
    }

    // Processing Time 윈도우용 집계 함수
    public static class TimeSlotAggregateFunction implements AggregateFunction<EcommerceEvent, TimeSlotAccumulator, UserFeatureVector> {

        @Override
        public TimeSlotAccumulator createAccumulator() {
            return new TimeSlotAccumulator();
        }

        @Override
        public TimeSlotAccumulator add(EcommerceEvent event, TimeSlotAccumulator accumulator) {
            int hour = event.getHourOfDay();
            if (hour >= 0 && hour < 24) {
                accumulator.hourlyCounts[hour]++;
                accumulator.userId = event.user_id;
            }
            return accumulator;
        }

        @Override
        public UserFeatureVector getResult(TimeSlotAccumulator accumulator) {
            UserFeatureVector features = new UserFeatureVector();
            features.user_id = accumulator.userId;
            features.timestamp = System.currentTimeMillis();

            // 시간대별 비율 계산
            features.time_slot_ratios = calculateTimeSlotRatios(accumulator.hourlyCounts);
            return features;
        }

        @Override
        public TimeSlotAccumulator merge(TimeSlotAccumulator a, TimeSlotAccumulator b) {
            TimeSlotAccumulator merged = new TimeSlotAccumulator();
            merged.userId = a.userId != null ? a.userId : b.userId;

            for (int i = 0; i < 24; i++) {
                merged.hourlyCounts[i] = a.hourlyCounts[i] + b.hourlyCounts[i];
            }
            return merged;
        }

        private Map<String, Double> calculateTimeSlotRatios(Integer[] hourlyCounts) {
            Map<String, Double> timeSlotRatios = new HashMap<>();

            int total = Arrays.stream(hourlyCounts).mapToInt(Integer::intValue).sum();
            if (total == 0) {
                timeSlotRatios.put("time_slot_0_6", 0.0);
                timeSlotRatios.put("time_slot_6_12", 0.0);
                timeSlotRatios.put("time_slot_12_18", 0.0);
                timeSlotRatios.put("time_slot_18_24", 0.0);
                return timeSlotRatios;
            }

            // 각 구간별 활동량 계산
            int slot0Count = 0, slot6Count = 0, slot12Count = 0, slot18Count = 0;

            for (int hour = 0; hour < 6; hour++) slot0Count += hourlyCounts[hour];
            for (int hour = 6; hour < 12; hour++) slot6Count += hourlyCounts[hour];
            for (int hour = 12; hour < 18; hour++) slot12Count += hourlyCounts[hour];
            for (int hour = 18; hour < 24; hour++) slot18Count += hourlyCounts[hour];

            timeSlotRatios.put("time_slot_0_6", (double) slot0Count / total);
            timeSlotRatios.put("time_slot_6_12", (double) slot6Count / total);
            timeSlotRatios.put("time_slot_12_18", (double) slot12Count / total);
            timeSlotRatios.put("time_slot_18_24", (double) slot18Count / total);

            return timeSlotRatios;
        }
    }

    // JSON 파서
    public static class JsonEventParser implements MapFunction<String, EcommerceEvent> {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public EcommerceEvent map(String json) throws Exception {
            try {
                JsonNode node = objectMapper.readTree(json);

                EcommerceEvent event = new EcommerceEvent();
                event.event_time = node.get("event_time").asText();
                event.event_type = node.get("event_type").asText();
                event.product_id = node.get("product_id").asText();
                event.category_id = node.get("category_id").asText();
                event.category_code = node.has("category_code") && !node.get("category_code").isNull()
                    ? node.get("category_code").asText() : null;
                event.brand = node.has("brand") && !node.get("brand").isNull()
                    ? node.get("brand").asText() : null;
                event.price = node.get("price").asDouble();
                event.user_id = node.get("user_id").asText();
                event.user_session = node.get("user_session").asText();
                return event;
            } catch (Exception e) {
                return null;
            }
        }
    }

    // Redis Sink
    public static class RedisSink extends RichSinkFunction<UserFeatureVector> {
        private transient JedisPool jedisPool;
        private final String redisHost;
        private final int redisPort;

        public RedisSink(String redisHost, int redisPort) {
            this.redisHost = redisHost;
            this.redisPort = redisPort;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            config.setMaxIdle(5);
            config.setMinIdle(1);
            config.setTestOnBorrow(true);

            jedisPool = new JedisPool(config, redisHost, redisPort, 2000);
        }

        @Override
        public void invoke(UserFeatureVector features, Context context) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                String key = "user_features:" + features.user_id;

                // HSET 방식으로 시간대 비율 피처만 저장
                Map<String, String> hashFields = new HashMap<>();
                hashFields.put("user_id", features.user_id);
                hashFields.put("timestamp", String.valueOf(features.timestamp));

                // 시간대 비율을 개별 필드로 저장
                hashFields.put("time_slot_0_6", String.valueOf(features.time_slot_ratios.get("time_slot_0_6")));
                hashFields.put("time_slot_6_12", String.valueOf(features.time_slot_ratios.get("time_slot_6_12")));
                hashFields.put("time_slot_12_18", String.valueOf(features.time_slot_ratios.get("time_slot_12_18")));
                hashFields.put("time_slot_18_24", String.valueOf(features.time_slot_ratios.get("time_slot_18_24")));

                // HSET으로 모든 필드를 한 번에 저장
                jedis.hset(key, hashFields);

                // TTL 설정 (7일)
                jedis.expire(key, 604800);

            } catch (Exception e) {
                System.err.println("Redis write failed for user " + features.user_id + ": " + e.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Web UI가 포함된 로컬 환경 생성
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);  // 병렬도 1로 설정
        env.enableCheckpointing(30000);

        // 체크포인트 저장소 설정 (프로젝트 루트 기준, 절대경로로 변환)
        String projectRoot = System.getProperty("user.dir");
        String checkpointDir = projectRoot + "/flink-checkpoints";
        env.getCheckpointConfig().setCheckpointStorage("file:///" + checkpointDir);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("ecommerce-events")
            .setGroupId("feature-engineering-group")
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(kafkaSource,
            WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<EcommerceEvent> eventStream = rawStream
            .map(new JsonEventParser())
            .filter(event -> event != null && event.user_id != null)
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<EcommerceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))  // 1초만 지연 허용
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis()));

        // Event Time 슬라이딩 윈도우로 시간대 피처 계산
        DataStream<UserFeatureVector> featureStream = eventStream
            .keyBy(event -> event.user_id)
            .window(SlidingEventTimeWindows.of(Time.hours(12), Time.minutes(1)))  // 5분 윈도우, 1분 슬라이드
            .aggregate(new TimeSlotAggregateFunction());

        featureStream.addSink(new RedisSink("localhost", 6379));

        featureStream.print("Features");

        env.execute("Ecommerce Time Slot Feature Pipeline");
    }
}