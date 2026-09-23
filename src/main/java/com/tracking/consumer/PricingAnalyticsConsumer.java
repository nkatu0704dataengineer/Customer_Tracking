package com.tracking.consumer;

import com.tracking.model.CustomerEvent;
import com.tracking.model.CustomerEvent.EventTypes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka Consumer phụ trách Dynamic Pricing (giảm giá động chống bỏ quên giỏ hàng)
 * và Funnel Analytics (phát hiện điểm gãy thanh toán / săn mã voucher).
 */
@Slf4j
public class PricingAnalyticsConsumer implements Runnable {

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_GROUP_ID = "pricing-analytics-group";
    public static final String TOPIC = "customer-events-raw";

    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * In-Memory State Store theo dõi ý định mua sắm của khách hàng per product.
     * Key định danh: acc_id + "_" + product_id
     */
    private final Map<String, IntentTrackingState> stateMap = new ConcurrentHashMap<>();

    /**
     * Đối tượng lưu trạng thái theo dõi ý định (Intent Tracking State).
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IntentTrackingState {
        private String accId;
        private String productId;
        private String originAction;      // ADD_TO_CART hoặc WISHLIST
        private String startSessionId;    // VD: "SS00000001"
        private int startSessionNumber;   // Giá trị số parse từ startSessionId, VD: 1
        private String startTime;         // ISO-8601
        @Builder.Default
        private int checkoutAttempts = 0; // Khởi tạo = 0
        @Builder.Default
        private boolean discountApplied = false; // Khởi tạo = false
    }

    public PricingAnalyticsConsumer() {
        this(DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_GROUP_ID);
    }

    public PricingAnalyticsConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.consumer = new KafkaConsumer<>(props);
    }

    /**
     * Helper method trích xuất số nguyên từ chuỗi "SS%08d" (VD: "SS00000004" -> 4).
     */
    public static int parseSessionNumber(String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return 0;
        }
        try {
            String digits = sessionId.replaceAll("\\D+", "");
            return digits.isEmpty() ? 0 : Integer.parseInt(digits);
        } catch (Exception e) {
            log.warn("Failed to parse session number from sessionId: {}", sessionId);
            return 0;
        }
    }

    public static String buildStateKey(String accId, String productId) {
        return accId + "_" + productId;
    }

    public Map<String, IntentTrackingState> getStateMap() {
        return stateMap;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            log.info("PricingAnalyticsConsumer subscribed to topic: {}", TOPIC);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        CustomerEvent event = CustomerEvent.fromJson(record.value());
                        processEvent(event);
                    } catch (Exception e) {
                        log.error("Failed to parse and process record value: {}", record.value(), e);
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                log.error("Consumer received WakeupException unexpectedly", e);
                throw e;
            }
        } catch (Exception e) {
            log.error("Unexpected error in PricingAnalyticsConsumer loop", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception ignored) {
            }
            consumer.close();
            log.info("PricingAnalyticsConsumer closed safely.");
        }
    }

    /**
     * Ma trận xử lý sự kiện (Event Processing Logic).
     */
    public void processEvent(CustomerEvent event) {
        if (event == null) {
            return;
        }

        String accId = event.getAccId();
        String sessionId = event.getSessionId();
        String productId = event.getProductId();
        String eventType = event.getEventType();
        String eventTime = event.getEventTime();

        if (accId == null || accId.isBlank()) {
            return;
        }

        // ---------------------------------------------------------------------
        // NHÁNH 3: KIỂM TRA DYNAMIC PRICING (Áp dụng cho mọi sự kiện của tài khoản đó)
        // ---------------------------------------------------------------------
        checkDynamicPricing(accId, sessionId);

        if (eventType == null) {
            return;
        }

        String stateKey = (productId != null && !productId.isBlank()) ? buildStateKey(accId, productId) : null;

        switch (eventType) {
            case EventTypes.ADD_TO_CART:
            case EventTypes.WISHLIST:
                // NHÁNH 1: eventType là ADD_TO_CART hoặc WISHLIST
                if (stateKey != null) {
                    int sessionNum = parseSessionNumber(sessionId);
                    IntentTrackingState state = IntentTrackingState.builder()
                            .accId(accId)
                            .productId(productId)
                            .originAction(eventType)
                            .startSessionId(sessionId)
                            .startSessionNumber(sessionNum)
                            .startTime(eventTime)
                            .checkoutAttempts(0)
                            .discountApplied(false)
                            .build();

                    stateMap.put(stateKey, state);
                    log.info("[INTENT CAPTURED] Acc: {} | Product: {} | Action: {} | Session: {} | Time: {}",
                            accId, productId, eventType, sessionId, eventTime);
                }
                break;

            case EventTypes.CHECKOUT:
                // NHÁNH 2: eventType là CHECKOUT
                if (stateKey != null) {
                    IntentTrackingState checkoutState = stateMap.compute(stateKey, (k, existingState) -> {
                        if (existingState == null) {
                            return IntentTrackingState.builder()
                                    .accId(accId)
                                    .productId(productId)
                                    .originAction(EventTypes.CHECKOUT)
                                    .startSessionId(sessionId)
                                    .startSessionNumber(parseSessionNumber(sessionId))
                                    .startTime(eventTime)
                                    .checkoutAttempts(1)
                                    .discountApplied(false)
                                    .build();
                        } else {
                            existingState.setCheckoutAttempts(existingState.getCheckoutAttempts() + 1);
                            return existingState;
                        }
                    });

                    // KIỂM TRA NGƯỠNG GÃY (Checkout Friction / Voucher Hunting)
                    if (checkoutState != null && checkoutState.getCheckoutAttempts() >= 2) {
                        log.warn("[ALERT - CHECKOUT FRICTION] Acc: {} | Product: {} | Attempted Checkout: {} times without paying! -> Action: Trigger Instant 20k Voucher/FreeShip to close deal!",
                                accId, productId, checkoutState.getCheckoutAttempts());
                    }
                }
                break;

            case EventTypes.PURCHASE:
                // NHÁNH 4: eventType là PURCHASE
                if (stateKey != null) {
                    IntentTrackingState state = stateMap.remove(stateKey);
                    if (state != null) {
                        log.info("[CONVERSION SUCCESS] Acc: {} | Product: {} | Purchased after {} checkout attempts! Removing from tracking.",
                                state.getAccId(), state.getProductId(), state.getCheckoutAttempts());
                    }
                }
                break;

            case EventTypes.REMOVE_FROM_CART:
                // NHÁNH 5: eventType là REMOVE_FROM_CART
                if (stateKey != null) {
                    IntentTrackingState state = stateMap.remove(stateKey);
                    if (state != null) {
                        log.info("[CART REMOVED] Acc: {} | Product: {} dropped from cart.", accId, productId);
                    }
                }
                break;

            default:
                // VIEW, CLICK, SEARCH,... không làm thay đổi trạng thái giỏ/intent
                break;
        }
    }

    /**
     * Kiểm tra Dynamic Pricing: nếu người dùng đã sang phiên mới và sản phẩm chưa được áp dụng giảm giá,
     * kích hoạt coupon 10% để chốt đơn giỏ hàng bỏ quên.
     */
    private void checkDynamicPricing(String accId, String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return;
        }

        int currentSessionNum = parseSessionNumber(sessionId);

        for (IntentTrackingState state : stateMap.values()) {
            if (accId.equals(state.getAccId())) {
                if (currentSessionNum > state.getStartSessionNumber() && !state.isDiscountApplied()) {
                    state.setDiscountApplied(true);
                    log.info("[DYNAMIC PRICING ACTIVATED] Acc: {} | Product: {} | Added in: {} | Current Session: {} -> Trigger 10% Extra Discount Coupon!",
                            state.getAccId(), state.getProductId(), state.getStartSessionId(), sessionId);
                }
            }
        }
    }

    /**
     * Dừng consumer một cách an toàn.
     */
    public void shutdown() {
        running.set(false);
        consumer.wakeup();
    }

    /**
     * Hàm main hoàn chỉnh để chạy độc lập.
     */
    public static void main(String[] args) {
        log.info("================================================================================");
        log.info("Starting Pricing Analytics & Dynamic Pricing Consumer");
        log.info("Topic: {} | Group ID: {} | Bootstrap: {}", TOPIC, DEFAULT_GROUP_ID, DEFAULT_BOOTSTRAP_SERVERS);
        log.info("================================================================================");

        PricingAnalyticsConsumer consumer = new PricingAnalyticsConsumer();

        // Đăng ký JVM Shutdown Hook để Graceful Shutdown khi nhận tín hiệu SIGINT/Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received (Ctrl+C). Initiating graceful shutdown for PricingAnalyticsConsumer...");
            consumer.shutdown();
        }, "pricing-analytics-shutdown-hook"));

        // Khởi động consumer loop
        consumer.run();
    }
}
