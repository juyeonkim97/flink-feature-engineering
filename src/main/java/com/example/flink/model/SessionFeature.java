package com.example.flink.model;

public class SessionFeature {
    public String user_session;
    public String user_id;
    public long timestamp;
    public int event_count;
    public long duration_seconds;

    @Override
    public String toString() {
        return String.format("SessionFeature{session=%s, user=%s, events=%d, duration=%ds}",
            user_session, user_id, event_count, duration_seconds);
    }
} 