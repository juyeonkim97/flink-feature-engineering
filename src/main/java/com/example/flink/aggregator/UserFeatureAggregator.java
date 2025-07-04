package com.example.flink.aggregator;

import com.example.flink.model.EcommerceEvent;
import com.example.flink.model.UserFeature;
import org.apache.flink.api.common.functions.AggregateFunction;

public class UserFeatureAggregator implements AggregateFunction<EcommerceEvent, UserAccumulator, UserFeature> {
    
    @Override
    public UserAccumulator createAccumulator() {
        return new UserAccumulator();
    }

    @Override
    public UserAccumulator add(EcommerceEvent event, UserAccumulator acc) {
        acc.user_id = event.user_id;
        switch (event.event_type) {
            case "view":
                acc.view_count++;
                acc.total_viewed_price += event.price;
                break;
            case "cart": acc.cart_count++; break;
            case "purchase": acc.purchase_count++; break;
        }
        return acc;
    }

    @Override
    public UserFeature getResult(UserAccumulator acc) {
        UserFeature feature = new UserFeature();
        feature.user_id = acc.user_id;
        feature.timestamp = System.currentTimeMillis();
        feature.view_count = acc.view_count;
        feature.cart_count = acc.cart_count;
        feature.purchase_count = acc.purchase_count;
        feature.avg_viewed_price = acc.view_count > 0 ? acc.total_viewed_price / acc.view_count : 0.0;
        return feature;
    }

    @Override
    public UserAccumulator merge(UserAccumulator a, UserAccumulator b) {
        UserAccumulator merged = new UserAccumulator();
        merged.user_id = a.user_id != null ? a.user_id : b.user_id;
        merged.view_count = a.view_count + b.view_count;
        merged.cart_count = a.cart_count + b.cart_count;
        merged.purchase_count = a.purchase_count + b.purchase_count;
        merged.total_viewed_price = a.total_viewed_price + b.total_viewed_price;
        return merged;
    }
} 