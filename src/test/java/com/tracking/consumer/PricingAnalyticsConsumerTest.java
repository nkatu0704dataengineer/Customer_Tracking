package com.tracking.consumer;

import com.tracking.model.CustomerEvent;
import com.tracking.model.CustomerEvent.EventTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PricingAnalyticsConsumerTest {

    private PricingAnalyticsConsumer consumer;

    @BeforeEach
    void setUp() {
        // Khởi tạo instance xử lý logic event (không gọi connect tới Kafka)
        consumer = new PricingAnalyticsConsumer("localhost:9092", "test-group");
    }

    @Test
    @DisplayName("Test parseSessionNumber trích xuất đúng số nguyên")
    void testParseSessionNumber() {
        assertEquals(1, PricingAnalyticsConsumer.parseSessionNumber("SS00000001"));
        assertEquals(4, PricingAnalyticsConsumer.parseSessionNumber("SS00000004"));
        assertEquals(123, PricingAnalyticsConsumer.parseSessionNumber("SS00000123"));
        assertEquals(0, PricingAnalyticsConsumer.parseSessionNumber(null));
        assertEquals(0, PricingAnalyticsConsumer.parseSessionNumber(""));
    }

    @Test
    @DisplayName("Nhánh 1: ADD_TO_CART & WISHLIST lưu trạng thái vào stateMap")
    void testAddToCartAndWishlist() {
        CustomerEvent event = CustomerEvent.builder()
                .accId("ACC00000001")
                .sessionId("SS00000001")
                .productId("PR001")
                .eventType(EventTypes.ADD_TO_CART)
                .eventTime("2026-09-22T10:00:00Z")
                .build();

        consumer.processEvent(event);

        String key = PricingAnalyticsConsumer.buildStateKey("ACC00000001", "PR001");
        PricingAnalyticsConsumer.IntentTrackingState state = consumer.getStateMap().get(key);

        assertNotNull(state);
        assertEquals("ACC00000001", state.getAccId());
        assertEquals("PR001", state.getProductId());
        assertEquals(EventTypes.ADD_TO_CART, state.getOriginAction());
        assertEquals("SS00000001", state.getStartSessionId());
        assertEquals(1, state.getStartSessionNumber());
        assertEquals(0, state.getCheckoutAttempts());
        assertFalse(state.isDiscountApplied());
    }

    @Test
    @DisplayName("Nhánh 2: CHECKOUT tăng số lần và cảnh báo Checkout Friction")
    void testCheckoutAttempts() {
        String accId = "ACC00000002";
        String productId = "PR005";
        String key = PricingAnalyticsConsumer.buildStateKey(accId, productId);

        CustomerEvent checkout1 = CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000001")
                .productId(productId)
                .eventType(EventTypes.CHECKOUT)
                .eventTime("2026-09-22T10:05:00Z")
                .build();

        consumer.processEvent(checkout1);
        assertEquals(1, consumer.getStateMap().get(key).getCheckoutAttempts());

        CustomerEvent checkout2 = CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000001")
                .productId(productId)
                .eventType(EventTypes.CHECKOUT)
                .eventTime("2026-09-22T10:06:00Z")
                .build();

        consumer.processEvent(checkout2);
        assertEquals(2, consumer.getStateMap().get(key).getCheckoutAttempts());
    }

    @Test
    @DisplayName("Nhánh 3: DYNAMIC PRICING kích hoạt khi chuyển sang session mới")
    void testDynamicPricingTrigger() {
        String accId = "ACC00000003";
        String productId = "PR010";
        String key = PricingAnalyticsConsumer.buildStateKey(accId, productId);

        // Session 1: Người dùng Add to Cart
        CustomerEvent addCart = CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000001")
                .productId(productId)
                .eventType(EventTypes.ADD_TO_CART)
                .eventTime("2026-09-22T10:00:00Z")
                .build();
        consumer.processEvent(addCart);
        assertFalse(consumer.getStateMap().get(key).isDiscountApplied());

        // Session 2: Người dùng quay lại duyệt sản phẩm khác (VIEW)
        CustomerEvent viewNextSession = CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000002")
                .productId("PR020")
                .eventType(EventTypes.VIEW)
                .eventTime("2026-09-22T11:00:00Z")
                .build();
        consumer.processEvent(viewNextSession);

        // Trạng thái của PR010 phải được bật discountApplied = true
        assertTrue(consumer.getStateMap().get(key).isDiscountApplied());
    }

    @Test
    @DisplayName("Nhánh 4 & 5: PURCHASE và REMOVE_FROM_CART xóa khỏi stateMap")
    void testPurchaseAndRemove() {
        String accId = "ACC00000004";
        String productId = "PR015";
        String key = PricingAnalyticsConsumer.buildStateKey(accId, productId);

        // Add
        consumer.processEvent(CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000001")
                .productId(productId)
                .eventType(EventTypes.ADD_TO_CART)
                .build());
        assertTrue(consumer.getStateMap().containsKey(key));

        // Purchase
        consumer.processEvent(CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000001")
                .productId(productId)
                .eventType(EventTypes.PURCHASE)
                .build());
        assertFalse(consumer.getStateMap().containsKey(key));

        // Add again and Remove
        consumer.processEvent(CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000001")
                .productId(productId)
                .eventType(EventTypes.ADD_TO_CART)
                .build());
        assertTrue(consumer.getStateMap().containsKey(key));

        consumer.processEvent(CustomerEvent.builder()
                .accId(accId)
                .sessionId("SS00000001")
                .productId(productId)
                .eventType(EventTypes.REMOVE_FROM_CART)
                .build());
        assertFalse(consumer.getStateMap().containsKey(key));
    }
}
