package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
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

        public Long getEventTimeMillis() {
            try {
                LocalDateTime kstTime = LocalDateTime.ofInstant(
                    Instant.parse(event_time),
                    ZoneOffset.of("+09:00")
                );
                return kstTime.toInstant(ZoneOffset.of("+09:00")).toEpochMilli();
            } catch (Exception e) {
                return null;
            }
        }
    }

    // 1시간 사용자 활동 피처
    public static class HourlyUserFeature {
        public String user_id;
        public long timestamp;
        public int view_count_1h;
        public int cart_count_1h;
        public int purchase_count_1h;

        @Override
        public String toString() {
            return String.format("HourlyFeature{user=%s, view=%d, cart=%d, purchase=%d}",
                user_id, view_count_1h, cart_count_1h, purchase_count_1h);
        }
    }

    // 10분 상품별 조회 피처
    public static class ProductViewFeature {
        public String user_id;
        public String product_id;
        public long timestamp;
        public int product_view_count_10m;

        @Override
        public String toString() {
            return String.format("ProductFeature{user=%s, product=%s, views=%d}",
                user_id, product_id, product_view_count_10m);
        }
    }

    // 세션 피처
    public static class SessionFeature {
        public String user_session;
        public String user_id;
        public long timestamp;
        public int total_events_in_session;
        public long session_duration_seconds;

        @Override
        public String toString() {
            return String.format("SessionFeature{session=%s, user=%s, events=%d, duration=%ds}",
                user_session, user_id, total_events_in_session, session_duration_seconds);
        }
    }

    // 1시간 사용자 활동 집계
    public static class HourlyUserAggregator implements AggregateFunction<EcommerceEvent, HourlyUserAccumulator, HourlyUserFeature> {

        @Override
        public HourlyUserAccumulator createAccumulator() {
            return new HourlyUserAccumulator();
        }

        @Override
        public HourlyUserAccumulator add(EcommerceEvent event, HourlyUserAccumulator acc) {
            acc.user_id = event.user_id;
            switch (event.event_type) {
                case "view":
                    acc.view_count++;
                    break;
                case "cart":
                    acc.cart_count++;
                    break;
                case "purchase":
                    acc.purchase_count++;
                    break;
            }
            return acc;
        }

        @Override
        public HourlyUserFeature getResult(HourlyUserAccumulator acc) {
            HourlyUserFeature feature = new HourlyUserFeature();
            feature.user_id = acc.user_id;
            feature.timestamp = System.currentTimeMillis();
            feature.view_count_1h = acc.view_count;
            feature.cart_count_1h = acc.cart_count;
            feature.purchase_count_1h = acc.purchase_count;
            return feature;
        }

        @Override
        public HourlyUserAccumulator merge(HourlyUserAccumulator a, HourlyUserAccumulator b) {
            HourlyUserAccumulator merged = new HourlyUserAccumulator();
            merged.user_id = a.user_id != null ? a.user_id : b.user_id;
            merged.view_count = a.view_count + b.view_count;
            merged.cart_count = a.cart_count + b.cart_count;
            merged.purchase_count = a.purchase_count + b.purchase_count;
            return merged;
        }
    }

    // 10분 상품 조회 집계
    public static class ProductViewAggregator implements AggregateFunction<EcommerceEvent, ProductViewAccumulator, ProductViewFeature> {

        @Override
        public ProductViewAccumulator createAccumulator() {
            return new ProductViewAccumulator();
        }

        @Override
        public ProductViewAccumulator add(EcommerceEvent event, ProductViewAccumulator acc) {
            if ("view".equals(event.event_type)) {
                acc.user_id = event.user_id;
                acc.product_id = event.product_id;
                acc.view_count++;
            }
            return acc;
        }

        @Override
        public ProductViewFeature getResult(ProductViewAccumulator acc) {
            ProductViewFeature feature = new ProductViewFeature();
            feature.user_id = acc.user_id;
            feature.product_id = acc.product_id;
            feature.timestamp = System.currentTimeMillis();
            feature.product_view_count_10m = acc.view_count;
            return feature;
        }

        @Override
        public ProductViewAccumulator merge(ProductViewAccumulator a, ProductViewAccumulator b) {
            ProductViewAccumulator merged = new ProductViewAccumulator();
            merged.user_id = a.user_id != null ? a.user_id : b.user_id;
            merged.product_id = a.product_id != null ? a.product_id : b.product_id;
            merged.view_count = a.view_count + b.view_count;
            return merged;
        }
    }

    // 세션 집계
    public static class SessionAggregator implements AggregateFunction<EcommerceEvent, SessionAccumulator, SessionFeature> {

        @Override
        public SessionAccumulator createAccumulator() {
            return new SessionAccumulator();
        }

        @Override
        public SessionAccumulator add(EcommerceEvent event, SessionAccumulator acc) {
            acc.user_session = event.user_session;
            acc.user_id = event.user_id;
            acc.event_count++;

            long eventTime = event.getEventTimeMillis();
            if (acc.first_event_time == 0) {
                acc.first_event_time = eventTime;
            }
            acc.last_event_time = eventTime;
            return acc;
        }

        @Override
        public SessionFeature getResult(SessionAccumulator acc) {
            SessionFeature feature = new SessionFeature();
            feature.user_session = acc.user_session;
            feature.user_id = acc.user_id;
            feature.timestamp = System.currentTimeMillis();
            feature.total_events_in_session = acc.event_count;
            feature.session_duration_seconds = (acc.last_event_time - acc.first_event_time) / 1000;
            return feature;
        }

        @Override
        public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
            SessionAccumulator merged = new SessionAccumulator();
            merged.user_session = a.user_session != null ? a.user_session : b.user_session;
            merged.user_id = a.user_id != null ? a.user_id : b.user_id;
            merged.event_count = a.event_count + b.event_count;
            merged.first_event_time = Math.min(a.first_event_time, b.first_event_time);
            merged.last_event_time = Math.max(a.last_event_time, b.last_event_time);
            return merged;
        }
    }

    // 누적기 클래스들
    public static class HourlyUserAccumulator {
        public String user_id;
        public int view_count = 0;
        public int cart_count = 0;
        public int purchase_count = 0;
    }

    public static class ProductViewAccumulator {
        public String user_id;
        public String product_id;
        public int view_count = 0;
    }

    public static class SessionAccumulator {
        public String user_session;
        public String user_id;
        public int event_count = 0;
        public long first_event_time = 0;
        public long last_event_time = 0;
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

                System.out.println("Parsed event: user=" + event.user_id + ", type=" + event.event_type +
                    ", product=" + event.product_id + ", session=" + event.user_session);
                return event;
            } catch (Exception e) {
                System.err.println("Failed to parse JSON: " + json + ", Error: " + e.getMessage());
                return null;
            }
        }
    }

    // Redis Sink들
    public static class HourlyFeatureSink extends RichSinkFunction<HourlyUserFeature> {
        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            jedisPool = new JedisPool(config, "localhost", 6379, 2000);
        }

        @Override
        public void invoke(HourlyUserFeature feature, Context context) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                String key = "hourly_features:" + feature.user_id;
                Map<String, String> fields = new HashMap<>();
                fields.put("user_id", feature.user_id);
                fields.put("timestamp", String.valueOf(feature.timestamp));
                fields.put("view_count_1h", String.valueOf(feature.view_count_1h));
                fields.put("cart_count_1h", String.valueOf(feature.cart_count_1h));
                fields.put("purchase_count_1h", String.valueOf(feature.purchase_count_1h));

                jedis.hset(key, fields);
                jedis.expire(key, 7200); // 2시간 TTL
                System.out.println("Saved hourly feature: " + key);
            }
        }

        @Override
        public void close() throws Exception {
            if (jedisPool != null) jedisPool.close();
        }
    }

    public static class ProductFeatureSink extends RichSinkFunction<ProductViewFeature> {
        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            jedisPool = new JedisPool(config, "localhost", 6379, 2000);
        }

        @Override
        public void invoke(ProductViewFeature feature, Context context) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                String key = "product_features:" + feature.user_id + ":" + feature.product_id;
                Map<String, String> fields = new HashMap<>();
                fields.put("user_id", feature.user_id);
                fields.put("product_id", feature.product_id);
                fields.put("timestamp", String.valueOf(feature.timestamp));
                fields.put("product_view_count_10m", String.valueOf(feature.product_view_count_10m));

                jedis.hset(key, fields);
                jedis.expire(key, 1200); // 20분 TTL
                System.out.println("Saved product feature: " + key);
            }
        }

        @Override
        public void close() throws Exception {
            if (jedisPool != null) jedisPool.close();
        }
    }

    public static class SessionFeatureSink extends RichSinkFunction<SessionFeature> {
        private transient JedisPool jedisPool;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(10);
            jedisPool = new JedisPool(config, "localhost", 6379, 2000);
        }

        @Override
        public void invoke(SessionFeature feature, Context context) throws Exception {
            try (Jedis jedis = jedisPool.getResource()) {
                String key = "session_features:" + feature.user_session;
                Map<String, String> fields = new HashMap<>();
                fields.put("user_session", feature.user_session);
                fields.put("user_id", feature.user_id);
                fields.put("timestamp", String.valueOf(feature.timestamp));
                fields.put("total_events_in_session", String.valueOf(feature.total_events_in_session));
                fields.put("session_duration_seconds", String.valueOf(feature.session_duration_seconds));

                jedis.hset(key, fields);
                jedis.expire(key, 3600); // 1시간 TTL
            }
        }

        @Override
        public void close() throws Exception {
            if (jedisPool != null) jedisPool.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);
        env.enableCheckpointing(30000);

        String projectRoot = System.getProperty("user.dir");
        String checkpointDir = projectRoot + "/flink-checkpoints";
        env.getCheckpointConfig().setCheckpointStorage("file:///" + checkpointDir);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("ecommerce-events")
            .setGroupId("feature-engineering-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<EcommerceEvent> eventStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map(new JsonEventParser())
            .filter(Objects::nonNull)
            .assignTimestampsAndWatermarks(WatermarkStrategy.<EcommerceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis()));

        // 1. 1시간 사용자 활동 피처
        DataStream<HourlyUserFeature> hourlyFeatures = eventStream
            .keyBy(event -> event.user_id)
            .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
            .aggregate(new HourlyUserAggregator());

        // 2. 10분 상품별 조회 피처
        DataStream<ProductViewFeature> productFeatures = eventStream
            .filter(event -> "view".equals(event.event_type))
            .keyBy(event -> event.user_id + "_" + event.product_id)
            .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
            .aggregate(new ProductViewAggregator());

        // 3. 세션 피처
        DataStream<SessionFeature> sessionFeatures = eventStream
            .keyBy(event -> event.user_session)
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .aggregate(new SessionAggregator());

        // Redis에 저장
        hourlyFeatures.addSink(new HourlyFeatureSink());
        productFeatures.addSink(new ProductFeatureSink());
        sessionFeatures.addSink(new SessionFeatureSink());

        // 콘솔 출력
        hourlyFeatures.print("HourlyFeatures");
        productFeatures.print("ProductFeatures");
        sessionFeatures.print("SessionFeatures");

        env.execute("Real-time Feature Engineering Pipeline");
    }
}