package com.example.flink.aggregator;

public class SessionAccumulator {
    public String user_session;
    public String user_id;
    public int event_count = 0;
    public long first_event_time = 0;
    public long last_event_time = 0;
} 