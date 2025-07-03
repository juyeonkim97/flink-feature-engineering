package com.example.flink;

import com.example.flink.aggregator.SessionAggregator;
import com.example.flink.aggregator.UserFeatureAggregator;
import com.example.flink.model.EcommerceEvent;
import com.example.flink.model.SessionFeature;
import com.example.flink.model.UserFeature;
import com.example.flink.sink.RedisSink;
import com.example.flink.util.JsonEventParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Objects;

public class UserFeatureJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1);
        env.enableCheckpointing(30000);

        // Kafka 소스 설정
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("ecommerce-events")
            .setGroupId("feature-engineering-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Event stream creation
        DataStream<EcommerceEvent> eventStream = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map(new JsonEventParser()).name("JSON Parser")
            .filter(Objects::nonNull).name("Null Filter")
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<EcommerceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis()));

        // 1. User 1-hour features (updated every 10 minutes)
        DataStream<UserFeature> userFeatures = eventStream
            .keyBy(event -> event.user_id)
            .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
            .aggregate(new UserFeatureAggregator()).name("User Feature Aggregator");

        // 2. Session features (30 minute inactivity timeout)
        DataStream<SessionFeature> sessionFeatures = eventStream
            .keyBy(event -> event.user_session)
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .aggregate(new SessionAggregator()).name("Session Feature Aggregator");

        // Redis storage
        userFeatures.addSink(new RedisSink<>("user_features", 7200)).name("Redis User Feature Sink");
        sessionFeatures.addSink(new RedisSink<>("session_features", 3600)).name("Redis Session Feature Sink");

        // Console output
        userFeatures.print("UserFeature").name("User Feature Print");
        sessionFeatures.print("SessionFeature").name("Session Feature Print");

        env.execute("Real-time Feature Engineering Pipeline");
    }
}