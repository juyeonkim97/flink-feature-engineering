package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
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
import java.util.stream.Collectors;

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
                return Instant.parse(event_time.replace(" UTC", "Z")).toEpochMilli();
            } catch (Exception e) {
                return System.currentTimeMillis();
            }
        }

        public int getHourOfDay() {
            try {
                return LocalDateTime.ofInstant(
                    Instant.parse(event_time.replace(" UTC", "Z")),
                    ZoneOffset.UTC
                ).getHour();
            } catch (Exception e) {
                return LocalDateTime.now().getHour();
            }
        }
    }

    // 피처 결과 모델
    public static class UserFeatureVector {
        public String user_id;
        public long timestamp;
        public List<String> top3_categories;
        public Map<String, Integer> category_counts;
        public int total_views_1h;
        public double category_diversity_score;
        public int peak_hour;
        public String preferred_time_slot;
        public boolean is_night_owl;
        public boolean is_early_bird;
        public Integer[] hourly_counts;
        public double activity_concentration;

        public UserFeatureVector() {
            this.top3_categories = new ArrayList<>();
            this.category_counts = new HashMap<>();
            this.hourly_counts = new Integer[24];
            // Integer 배열 초기화
            for (int i = 0; i < 24; i++) {
                this.hourly_counts[i] = 0;
            }
        }
        
        @Override
        public String toString() {
            return String.format("UserFeature{user=%s, categories=%s, total_views=%d, peak=%d, slot=%s, diversity=%.2f}",
                user_id, top3_categories, total_views_1h, peak_hour, preferred_time_slot, category_diversity_score);
        }
    }

    // 카테고리 뷰 데이터 클래스
    public static class CategoryView {
        public String category;
        public long timestamp;
        public double weight;

        public CategoryView() {
        }

        public CategoryView(String category, long timestamp, double weight) {
            this.category = category;
            this.timestamp = timestamp;
            this.weight = weight;
        }
    }

    // ✅ 올바른 KeyedProcessFunction 상속
    public static class FeatureProcessor extends KeyedProcessFunction<String, EcommerceEvent, UserFeatureVector> {

        // State descriptors
        private transient ListStateDescriptor<CategoryView> categoryViewsDescriptor;
        private transient ValueStateDescriptor<Integer[]> hourlyCountsDescriptor;
        private transient ValueStateDescriptor<Long> lastFeatureUpdateDescriptor;

        // State 선언
        private transient ListState<CategoryView> categoryViewsState;
        private transient ValueState<Integer[]> hourlyCountsState;
        private transient ValueState<Long> lastFeatureUpdateState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // State descriptors 초기화 및 state 생성
            categoryViewsDescriptor = new ListStateDescriptor<>("category-views", CategoryView.class);
            
            // TypeInformation 생성 (래퍼 클래스 배열 사용)
            TypeInformation<Integer[]> intArrayType = Types.OBJECT_ARRAY(Types.INT);
            TypeInformation<Long> longType = Types.LONG;
            
            hourlyCountsDescriptor = new ValueStateDescriptor<>("hourly-counts", intArrayType);
            lastFeatureUpdateDescriptor = new ValueStateDescriptor<>("last-update", longType);

            // StreamingRuntimeContext로 캐스팅하여 state 접근
            StreamingRuntimeContext streamingContext = (StreamingRuntimeContext) getRuntimeContext();
            categoryViewsState = streamingContext.getListState(categoryViewsDescriptor);
            hourlyCountsState = streamingContext.getState(hourlyCountsDescriptor);
            lastFeatureUpdateState = streamingContext.getState(lastFeatureUpdateDescriptor);
        }

        @Override
        public void processElement(EcommerceEvent event, Context ctx,
                                   Collector<UserFeatureVector> out) throws Exception {

            long currentTime = ctx.timestamp();
            Long lastUpdate = lastFeatureUpdateState.value();
            if (lastUpdate == null) lastUpdate = 0L;

            // 1. 시간대 패턴 업데이트
            updateHourlyPattern(event, currentTime);

            // 2. 카테고리 관심도 업데이트
            if ("view".equals(event.event_type) || "cart".equals(event.event_type)) {
                updateCategoryInterest(event, currentTime);
            }

            // 3. 피처 출력 (5분마다)
            if (lastUpdate == 0L || (currentTime - lastUpdate) > 300000) {
                UserFeatureVector features = calculateFeatures(event.user_id, currentTime);
                out.collect(features);
                lastFeatureUpdateState.update(currentTime);
            }
        }

        private void updateHourlyPattern(EcommerceEvent event, long timestamp) throws Exception {
            Integer[] hourlyCounts = hourlyCountsState.value();
            if (hourlyCounts == null) {
                hourlyCounts = new Integer[24];
                for (int i = 0; i < 24; i++) {
                    hourlyCounts[i] = 0;
                }
            }

            int hour = event.getHourOfDay();
            if (hour >= 0 && hour < 24) {
                hourlyCounts[hour]++;
                hourlyCountsState.update(hourlyCounts);
            }
        }

        private void updateCategoryInterest(EcommerceEvent event, long timestamp) throws Exception {
            if (event.category_code == null || event.category_code.trim().isEmpty()) {
                return;
            }

            List<CategoryView> views = new ArrayList<>();
            long oneHourAgo = timestamp - 3600000; // 1시간

            // 기존 뷰 필터링 (1시간 이내만 유지)
            if (categoryViewsState.get() != null) {
                for (CategoryView view : categoryViewsState.get()) {
                    if (view.timestamp > oneHourAgo) {
                        views.add(view);
                    }
                }
            }

            // 새 뷰 추가 (cart 이벤트는 가중치 2배)
            double weight = "cart".equals(event.event_type) ? 2.0 : 1.0;
            views.add(new CategoryView(event.category_code, timestamp, weight));

            categoryViewsState.update(views);
        }

        private UserFeatureVector calculateFeatures(String userId, long timestamp) throws Exception {
            UserFeatureVector features = new UserFeatureVector();
            features.user_id = userId;
            features.timestamp = timestamp;

            // Feature 1: 카테고리 관심도 계산
            calculateCategoryFeatures(features);

            // Feature 2: 시간대 패턴 계산
            calculateTimePatternFeatures(features);

            return features;
        }

        private void calculateCategoryFeatures(UserFeatureVector features) throws Exception {
            List<CategoryView> views = new ArrayList<>();
            if (categoryViewsState.get() != null) {
                for (CategoryView view : categoryViewsState.get()) {
                    views.add(view);
                }
            }

            if (views.isEmpty()) {
                return;
            }

            // 카테고리별 가중치 합계 계산
            Map<String, Double> categoryScores = new HashMap<>();
            for (CategoryView view : views) {
                // 시간 가중치 적용 (최근일수록 높은 점수)
                long timeDiff = features.timestamp - view.timestamp;
                double timeWeight = Math.exp(-timeDiff / 1800000.0); // 30분 half-life
                double totalScore = view.weight * timeWeight;

                categoryScores.merge(view.category, totalScore, Double::sum);
            }

            // Top 3 카테고리
            features.top3_categories = categoryScores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(3)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

            // 카테고리 카운트 (정수로 변환)
            for (Map.Entry<String, Double> entry : categoryScores.entrySet()) {
                features.category_counts.put(entry.getKey(), entry.getValue().intValue());
            }

            features.total_views_1h = views.size();

            // 다양성 점수 계산
            features.category_diversity_score = calculateDiversityScore(categoryScores);
        }

        private void calculateTimePatternFeatures(UserFeatureVector features) throws Exception {
            Integer[] hourlyCounts = hourlyCountsState.value();
            if (hourlyCounts == null) {
                hourlyCounts = new Integer[24];
                for (int i = 0; i < 24; i++) {
                    hourlyCounts[i] = 0;
                }
            }

            features.hourly_counts = Arrays.copyOf(hourlyCounts, 24);

            // 피크 시간 계산
            features.peak_hour = findPeakHour(hourlyCounts);
            features.preferred_time_slot = getTimeSlot(features.peak_hour);

            // 특성 분석
            features.is_night_owl = isNightOwl(hourlyCounts);
            features.is_early_bird = isEarlyBird(hourlyCounts);

            // 활동 집중도
            features.activity_concentration = calculateConcentration(hourlyCounts);
        }

        private double calculateDiversityScore(Map<String, Double> categoryScores) {
            double total = categoryScores.values().stream().mapToDouble(Double::doubleValue).sum();
            if (total == 0) return 0.0;

            double entropy = 0.0;
            for (double score : categoryScores.values()) {
                if (score > 0) {
                    double probability = score / total;
                    entropy -= probability * Math.log(probability);
                }
            }

            return categoryScores.size() > 1 ? entropy / Math.log(categoryScores.size()) : 0.0;
        }

        private int findPeakHour(Integer[] hourlyCounts) {
            int maxHour = 0;
            for (int i = 1; i < 24; i++) {
                if (hourlyCounts[i] > hourlyCounts[maxHour]) {
                    maxHour = i;
                }
            }
            return maxHour;
        }

        private String getTimeSlot(int hour) {
            if (hour >= 6 && hour < 12) return "morning";
            else if (hour >= 12 && hour < 18) return "afternoon";
            else if (hour >= 18 && hour < 22) return "evening";
            else return "night";
        }

        private boolean isNightOwl(Integer[] hourlyCounts) {
            int nightActivity = 0;
            for (int hour = 22; hour < 24; hour++) nightActivity += hourlyCounts[hour];
            for (int hour = 0; hour < 6; hour++) nightActivity += hourlyCounts[hour];

            int total = Arrays.stream(hourlyCounts).mapToInt(Integer::intValue).sum();
            return total > 10 && (double) nightActivity / total > 0.3;
        }

        private boolean isEarlyBird(Integer[] hourlyCounts) {
            int morningActivity = 0;
            for (int hour = 6; hour < 10; hour++) morningActivity += hourlyCounts[hour];

            int total = Arrays.stream(hourlyCounts).mapToInt(Integer::intValue).sum();
            return total > 10 && (double) morningActivity / total > 0.4;
        }

        private double calculateConcentration(Integer[] hourlyCounts) {
            int total = Arrays.stream(hourlyCounts).mapToInt(Integer::intValue).sum();
            if (total == 0) return 0.0;

            double entropy = 0.0;
            for (int count : hourlyCounts) {
                if (count > 0) {
                    double probability = (double) count / total;
                    entropy -= probability * Math.log(probability);
                }
            }

            double maxEntropy = Math.log(24);
            return Math.max(0.0, 1.0 - (entropy / maxEntropy));
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
                System.err.println("Failed to parse JSON: " + json + ", Error: " + e.getMessage());
                return null;
            }
        }
    }

    public static class RedisSink extends RichSinkFunction<UserFeatureVector> {
        private transient JedisPool jedisPool;
        private final String redisHost;
        private final int redisPort;
        private static final ObjectMapper objectMapper = new ObjectMapper();

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

                // HSET 방식으로 각 필드를 개별 저장
                Map<String, String> hashFields = new HashMap<>();
                hashFields.put("user_id", features.user_id);
                hashFields.put("timestamp", String.valueOf(features.timestamp));
                hashFields.put("total_views_1h", String.valueOf(features.total_views_1h));
                hashFields.put("category_diversity_score", String.valueOf(features.category_diversity_score));
                hashFields.put("peak_hour", String.valueOf(features.peak_hour));
                hashFields.put("preferred_time_slot", features.preferred_time_slot);
                hashFields.put("is_night_owl", String.valueOf(features.is_night_owl));
                hashFields.put("is_early_bird", String.valueOf(features.is_early_bird));
                hashFields.put("activity_concentration", String.valueOf(features.activity_concentration));
                
                // 복잡한 객체는 JSON으로 저장
                hashFields.put("top3_categories", objectMapper.writeValueAsString(features.top3_categories));
                hashFields.put("category_counts", objectMapper.writeValueAsString(features.category_counts));

                // HSET으로 모든 필드를 한 번에 저장
                jedis.hset(key, hashFields);
                
                // TTL 설정 (7일)
                jedis.expire(key, 604800);

                // 카테고리별 인덱싱
                for (String category : features.top3_categories) {
                    jedis.sadd("category_users:" + category, features.user_id);
                    jedis.expire("category_users:" + category, 86400);
                }

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
        
        env.enableCheckpointing(30000);

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
                .<EcommerceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis()));

        DataStream<UserFeatureVector> featureStream = eventStream
            .keyBy(event -> event.user_id)
            .process(new FeatureProcessor());

        featureStream.addSink(new RedisSink("localhost", 6379));

        featureStream.print("Features");

        env.execute("Ecommerce Feature Engineering Pipeline");
    }
}