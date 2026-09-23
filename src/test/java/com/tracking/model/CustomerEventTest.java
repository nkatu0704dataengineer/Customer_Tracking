package com.tracking.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit Test kiểm tra tính đúng đắn của CustomerEvent theo DB Schema mới:
 * - Khởi tạo với Builder theo các trường chuẩn hóa
 * - JSON Serialization (toJsonString) ra định dạng snake_case
 * - JSON Deserialization (fromJson) và so sánh toàn bộ các trường
 * - Kiểm tra Kafka Partition Key helper
 * - Kiểm tra danh sách hằng số EventTypes chuẩn
 */
class CustomerEventTest {

    private static final Logger log = LoggerFactory.getLogger(CustomerEventTest.class);

    @Test
    @DisplayName("Should serialize CustomerEvent to valid snake_case JSON and deserialize back correctly")
    void testSerializationAndDeserialization() {
        // 1. Khởi tạo đối tượng CustomerEvent với dữ liệu mẫu chuẩn
        CustomerEvent originalEvent = CustomerEvent.builder()
                .accId("ACC00000001")
                .sessionId("SS00000001")
                .eventTime("2024-02-09T00:09:00.000+00:00")
                .eventType(CustomerEvent.EventTypes.VIEW)
                .productId("PR630")
                .deviceType("Mobile")
                .timeOnPageMinutes(33)
                .createdAt("2024-02-09T00:09:00.000+00:00")
                .updatedAt("2026-09-22T01:47:53.464+00:00")
                .build();

        // 2. Kiểm tra helper Kafka Key
        assertEquals("ACC00000001", originalEvent.toKafkaKey(), "Kafka Key must equal accId");
        assertEquals("ACC00000001", originalEvent.getKafkaPartitionKey(), "Kafka Partition Key must equal accId");

        // 3. Serialize sang JSON String
        String json = originalEvent.toJsonString();
        log.info("Serialized CustomerEvent JSON Output:\n{}", json);

        assertNotNull(json, "JSON output must not be null");

        // 4. Kiểm tra chuỗi JSON sinh ra phải chứa đầy đủ các key snake_case chuẩn DB
        assertTrue(json.contains("\"acc_id\""), "JSON must contain 'acc_id'");
        assertTrue(json.contains("\"session_id\""), "JSON must contain 'session_id'");
        assertTrue(json.contains("\"event_time\""), "JSON must contain 'event_time'");
        assertTrue(json.contains("\"event_type\""), "JSON must contain 'event_type'");
        assertTrue(json.contains("\"product_id\""), "JSON must contain 'product_id'");
        assertTrue(json.contains("\"device_type\""), "JSON must contain 'device_type'");
        assertTrue(json.contains("\"time_on_page_minutes\""), "JSON must contain 'time_on_page_minutes'");
        assertTrue(json.contains("\"created_at\""), "JSON must contain 'created_at'");
        assertTrue(json.contains("\"updated_at\""), "JSON must contain 'updated_at'");

        // 5. Deserialize ngược lại thành đối tượng CustomerEvent
        CustomerEvent deserializedEvent = CustomerEvent.fromJson(json);
        assertNotNull(deserializedEvent, "Deserialized event must not be null");

        // 6. So sánh chính xác giá trị của TẤT CẢ các trường giữa đối tượng gốc và đối tượng khôi phục
        assertEquals(originalEvent.getAccId(), deserializedEvent.getAccId(), "accId must match");
        assertEquals(originalEvent.getSessionId(), deserializedEvent.getSessionId(), "sessionId must match");
        assertEquals(originalEvent.getEventTime(), deserializedEvent.getEventTime(), "eventTime must match");
        assertEquals(originalEvent.getEventType(), deserializedEvent.getEventType(), "eventType must match");
        assertEquals(originalEvent.getProductId(), deserializedEvent.getProductId(), "productId must match");
        assertEquals(originalEvent.getDeviceType(), deserializedEvent.getDeviceType(), "deviceType must match");
        assertEquals(originalEvent.getTimeOnPageMinutes(), deserializedEvent.getTimeOnPageMinutes(), "timeOnPageMinutes must match");
        assertEquals(originalEvent.getCreatedAt(), deserializedEvent.getCreatedAt(), "createdAt must match");
        assertEquals(originalEvent.getUpdatedAt(), deserializedEvent.getUpdatedAt(), "updatedAt must match");
    }

    @Test
    @DisplayName("Should verify all standard uppercase EventTypes constants")
    void testEventTypesConstant() {
        assertEquals("VIEW", CustomerEvent.EventTypes.VIEW);
        assertEquals("ADD_TO_CART", CustomerEvent.EventTypes.ADD_TO_CART);
        assertEquals("SEARCH", CustomerEvent.EventTypes.SEARCH);
        assertEquals("WISHLIST", CustomerEvent.EventTypes.WISHLIST);
        assertEquals("CLICK", CustomerEvent.EventTypes.CLICK);
        assertEquals("CHECKOUT", CustomerEvent.EventTypes.CHECKOUT);
        assertEquals("REMOVE_FROM_CART", CustomerEvent.EventTypes.REMOVE_FROM_CART);
        assertEquals("PURCHASE", CustomerEvent.EventTypes.PURCHASE);
    }
}
