package com.example.flink.aggregator;

import com.example.flink.model.EcommerceEvent;
import com.example.flink.model.SessionFeature;
import org.apache.flink.api.common.functions.AggregateFunction;

public class SessionAggregator implements AggregateFunction<EcommerceEvent, SessionAccumulator, SessionFeature> {
    
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
        feature.event_count = acc.event_count;
        feature.duration_seconds = (acc.last_event_time - acc.first_event_time) / 1000;
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