package com.example.flink.model;

import java.time.Instant;

public class EcommerceEvent {
    public String event_time;
    public String event_type;
    public String user_id;
    public double price;

    public Long getEventTimeMillis() {
        try {
            return Instant.parse(event_time).toEpochMilli();
        } catch (Exception e) {
            return null;
        }
    }
} 