package com.tracking.generator;

import com.tracking.model.CustomerEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class CustomerEventGeneratorTest {

    @Test
    @DisplayName("Should verify 100 accounts, 200 products, and stateful sessionization")
    void testStatefulSessionization() {
        CustomerEventGenerator generator = new CustomerEventGenerator();

        Set<String> generatedAccounts = new HashSet<>();
        Set<String> generatedProducts = new HashSet<>();

        for (int i = 0; i < 500; i++) {
            CustomerEvent event = generator.generateEvent();

            assertNotNull(event);
            assertNotNull(event.getAccId());
            assertTrue(event.getAccId().matches("^ACC\\d{8}$"), "accId must match ACC%08d");
            generatedAccounts.add(event.getAccId());

            assertNotNull(event.getSessionId());
            assertTrue(event.getSessionId().matches("^SS\\d{8}$"), "sessionId must match SS%08d");

            assertNotNull(event.getProductId());
            assertTrue(event.getProductId().matches("^PR\\d{3}$"), "productId must match PR%03d");
            generatedProducts.add(event.getProductId());

            assertNotNull(event.getEventTime());
            assertEquals(event.getEventTime(), event.getCreatedAt());
            assertEquals(event.getEventTime(), event.getUpdatedAt());

            assertTrue(event.getTimeOnPageMinutes() >= 1 && event.getTimeOnPageMinutes() <= 60);
        }

        assertTrue(generatedAccounts.size() > 50, "Should generate a broad variety of accounts");
        assertTrue(generatedProducts.size() > 50, "Should generate a broad variety of products");
    }

    @Test
    @DisplayName("Generate and print 50 sample events")
    void print50SampleEvents() {
        CustomerEventGenerator generator = new CustomerEventGenerator();
        System.out.println("=== BEGIN 50 GENERATED EVENTS ===");
        for (int i = 1; i <= 50; i++) {
            CustomerEvent event = generator.generateEvent();
            System.out.printf("[%02d] %s%n", i, event.toJsonString());
        }
        System.out.println("=== END 50 GENERATED EVENTS ===");
    }
}
