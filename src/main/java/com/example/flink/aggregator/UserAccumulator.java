package com.example.flink.aggregator;

public class UserAccumulator {
    public String user_id;
    public int view_count = 0;
    public int cart_count = 0;
    public int purchase_count = 0;
    public double total_viewed_price = 0.0; // 조회한 상품들의 가격 합계
} 