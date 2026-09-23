package com.tracking.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

/**
 * Lớp đại diện cho một sự kiện hành vi của khách hàng (Customer Tracking Event).
 * <p>
 * Schema đã được tái cấu trúc (refactor) chuẩn hóa theo thứ tự và tên cột của bảng cơ sở dữ liệu (Database Schema)
 * phục vụ ingestion vào Data Lake / Data Warehouse (Snowflake, BigQuery, ClickHouse, PostgreSQL) thông qua Kafka.
 *
 * <p><b>Chi tiết cấu hình:</b>
 * <ul>
 *     <li><b>@JsonPropertyOrder</b>: Cố định tuyệt đối thứ tự tuần tự của các thuộc tính khi serialize ra JSON:
 *         acc_id -> session_id -> event_time -> event_type -> product_id -> device_type ->
 *         time_on_page_minutes -> created_at -> updated_at</li>
 *     <li><b>@JsonProperty</b>: Ánh xạ chính xác các trường camelCase của Java sang snake_case tương ứng với cột DB.</li>
 *     <li><b>acc_id</b>: BẮT BUỘC dùng làm Kafka Message Key để bảo đảm thứ tự sự kiện (Strict Ordering) theo từng tài khoản.</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonPropertyOrder({
        "acc_id",
        "session_id",
        "event_time",
        "event_type",
        "product_id",
        "device_type",
        "time_on_page_minutes",
        "created_at",
        "updated_at"
})
@Slf4j
public class CustomerEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Singleton ObjectMapper tái sử dụng xuyên suốt pipeline, đảm bảo Thread-Safe và hiệu năng cao.
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // =========================================================================
    // 1. Danh sách chuẩn các loại sự kiện (EventTypes)
    // =========================================================================
    public static final class EventTypes {
        public static final String VIEW             = "VIEW";
        public static final String ADD_TO_CART      = "ADD_TO_CART";
        public static final String SEARCH           = "SEARCH";
        public static final String WISHLIST         = "WISHLIST";
        public static final String CLICK            = "CLICK";
        public static final String CHECKOUT         = "CHECKOUT";
        public static final String REMOVE_FROM_CART = "REMOVE_FROM_CART";
        public static final String PURCHASE         = "PURCHASE";

        private EventTypes() {}
    }

    // =========================================================================
    // 2. Database Schema Fields (Thứ tự đúng chuẩn cột DB & snake_case)
    // =========================================================================

    /**
     * Mã định danh tài khoản khách hàng (Account ID).
     * BẮT BUỘC dùng làm Kafka Message Key để partition hashing và đảm bảo ordering per customer.
     */
    @JsonProperty("acc_id")
    private String accId;

    /**
     * Mã định danh phiên truy cập của người dùng (Session ID).
     */
    @JsonProperty("session_id")
    private String sessionId;

    /**
     * Thời điểm xảy ra sự kiện theo chuẩn ISO-8601 (VD: "2024-02-09T00:09:00.000+00:00").
     * Đóng vai trò là Event Time trong stream processing.
     */
    @JsonProperty("event_time")
    private String eventTime;

    /**
     * Loại hành vi sự kiện (VIEW, ADD_TO_CART, SEARCH, WISHLIST, CLICK, CHECKOUT, REMOVE_FROM_CART, PURCHASE).
     */
    @JsonProperty("event_type")
    private String eventType;

    /**
     * Mã sản phẩm liên quan đến sự kiện (Product ID).
     */
    @JsonProperty("product_id")
    private String productId;

    /**
     * Loại thiết bị người dùng sử dụng (VD: MOBILE, DESKTOP, TABLET).
     */
    @JsonProperty("device_type")
    private String deviceType;

    /**
     * Thời gian người dùng ở lại trên trang tính theo đơn vị phút.
     */
    @JsonProperty("time_on_page_minutes")
    private Integer timeOnPageMinutes;

    /**
     * Thời điểm bản ghi được tạo trong hệ thống theo định dạng ISO-8601.
     */
    @JsonProperty("created_at")
    private String createdAt;

    /**
     * Thời điểm bản ghi được cập nhật lần cuối theo định dạng ISO-8601.
     */
    @JsonProperty("updated_at")
    private String updatedAt;

    // =========================================================================
    // 3. Helper Methods
    // =========================================================================

    /**
     * Helper method trả về Kafka Message Key (`accId`).
     * Dùng trực tiếp khi tạo {@code ProducerRecord<String, String>(topic, event.toKafkaKey(), event.toJsonString())}.
     *
     * @return Chuỗi accId làm Kafka Partition Key
     */
    @JsonIgnore
    public String toKafkaKey() {
        return this.accId;
    }

    /**
     * Helper alias cho toKafkaKey() để linh hoạt cú pháp khi tích hợp với Kafka Producer.
     *
     * @return Chuỗi accId làm Kafka Partition Key
     */
    @JsonIgnore
    public String getKafkaPartitionKey() {
        return this.accId;
    }

    /**
     * Chuyển đổi đối tượng CustomerEvent hiện tại thành chuỗi JSON String theo đúng thứ tự các cột DB.
     *
     * @return Chuỗi JSON String đã format snake_case và sắp xếp thứ tự trường
     * @throws RuntimeException nếu gặp lỗi serialization
     */
    public String toJsonString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize CustomerEvent for accId: {}, eventType: {}", accId, eventType, e);
            throw new RuntimeException("Error serializing CustomerEvent to JSON string", e);
        }
    }

    /**
     * Chuyển đổi chuỗi JSON String thành đối tượng CustomerEvent.
     *
     * @param jsonString Chuỗi JSON nhận từ Kafka Consumer hoặc REST API
     * @return Đối tượng CustomerEvent tương ứng
     * @throws RuntimeException nếu JSON sai format hoặc deserialization thất bại
     */
    public static CustomerEvent fromJson(String jsonString) {
        try {
            return OBJECT_MAPPER.readValue(jsonString, CustomerEvent.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize JSON string to CustomerEvent: {}", jsonString, e);
            throw new RuntimeException("Error deserializing JSON to CustomerEvent", e);
        }
    }
}
