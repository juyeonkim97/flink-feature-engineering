package com.example.flink.util;

import com.example.flink.model.EcommerceEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonEventParser implements MapFunction<String, EcommerceEvent> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public EcommerceEvent map(String json) throws Exception {
        try {
            JsonNode node = objectMapper.readTree(json);
            EcommerceEvent event = new EcommerceEvent();
            event.event_time = node.get("event_time").asText();
            event.event_type = node.get("event_type").asText();
            event.product_id = node.get("product_id").asText();
            event.user_id = node.get("user_id").asText();
            event.user_session = node.get("user_session").asText();
            event.price = node.get("price").asDouble();
            return event;
        } catch (Exception e) {
            System.err.println("JSON 파싱 실패: " + json);
            return null;
        }
    }
} 