package com.example.flink.model;

public class UserFeature {
    public String user_id;
    public long timestamp;
    public int view_count;
    public int cart_count;
    public int purchase_count;
    public double avg_viewed_price;

    @Override
    public String toString() {
        return String.format("UserFeature{user=%s, view=%d, cart=%d, purchase=%d, avgViewedPrice=%.2f}",
            user_id, view_count, cart_count, purchase_count, avg_viewed_price);
    }
} 