package com.tracking;

import com.tracking.consumer.PricingAnalyticsConsumer;
import com.tracking.consumer.RecommendationConsumer;
import com.tracking.generator.CustomerEventGenerator;
import com.tracking.model.CustomerEvent;
import com.tracking.producer.CustomerEventProducer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ứng dụng chính (Main Application) điều phối luồng sinh dữ liệu và phát sự kiện vào Kafka,
 * đồng thời chạy song song 2 Kafka Consumer phục vụ Real-time Recommendation & Dynamic Pricing Analytics.
 * <p>
 * Luồng hoạt động:
 * <ol>
 *     <li>Khởi tạo {@link CustomerEventGenerator} và {@link CustomerEventProducer}.</li>
 *     <li>Khởi tạo và kích hoạt 2 Consumer độc lập trên các background thread riêng:
 *         <ul>
 *             <li>{@link RecommendationConsumer} (recommendation-group)</li>
 *             <li>{@link PricingAnalyticsConsumer} (pricing-analytics-group)</li>
 *         </ul>
 *     </li>
 *     <li>Đăng ký JVM Shutdown Hook để bắt tín hiệu dừng (SIGINT / Ctrl+C), bảo đảm đóng an toàn Producer & cả 2 Consumer.</li>
 *     <li>Vòng lặp sinh và bắn sự kiện liên tục với tốc độ ổn định ~10 events/giây (sleep 100ms).</li>
 * </ol>
 */
@Slf4j
public class CustomerTrackingApp {

    private static final long EVENT_INTERVAL_MS = 100L; // 100ms = 10 events/second
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        log.info("================================================================================");
        log.info("Starting Customer Tracking Pipeline - Full Integrated Streaming System");
        log.info("Components: Generator | Producer | RecommendationConsumer | PricingConsumer");
        log.info("================================================================================");

        final AtomicBoolean running = new AtomicBoolean(true);
        final CustomerEventGenerator generator = new CustomerEventGenerator();
        final CustomerEventProducer producer;
        final RecommendationConsumer recommendationConsumer;
        final PricingAnalyticsConsumer pricingConsumer;

        try {
            // 1. Khởi tạo Producer
            producer = new CustomerEventProducer();

            // 2. Khởi tạo & chạy RecommendationConsumer trên thread riêng
            recommendationConsumer = new RecommendationConsumer(BOOTSTRAP_SERVERS, "recommendation-group");
            Thread recThread = new Thread(recommendationConsumer, "recommendation-consumer-thread");
            recThread.start();
            log.info("RecommendationConsumer thread started successfully.");

            // 3. Khởi tạo & chạy PricingAnalyticsConsumer trên thread riêng
            pricingConsumer = new PricingAnalyticsConsumer(BOOTSTRAP_SERVERS, "pricing-analytics-group");
            Thread pricingThread = new Thread(pricingConsumer, "pricing-analytics-consumer-thread");
            pricingThread.start();
            log.info("PricingAnalyticsConsumer thread started successfully.");

        } catch (Exception e) {
            log.error("Failed to initialize pipeline components. Ensure Kafka is running at localhost:9092: {}",
                    e.getMessage(), e);
            return;
        }

        // 4. Đăng ký JVM Shutdown Hook để Graceful Shutdown khi bấm Ctrl+C hoặc tiến trình bị tắt
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received (SIGINT/Ctrl+C). Initiating graceful shutdown...");
            running.set(false);
            producer.close();
            recommendationConsumer.shutdown();
            pricingConsumer.shutdown();
            log.info("Customer Tracking Pipeline stopped successfully.");
        }, "shutdown-hook-thread"));

        long eventCount = 0;
        log.info("Event generation loop started. Producing ~10 events/sec to Kafka. Press Ctrl+C to terminate.");

        // 5. Vòng lặp phát sự kiện liên tục
        while (running.get()) {
            try {
                // Sinh sự kiện giả lập
                CustomerEvent event = generator.generateEvent();

                // Bắn sự kiện vào Kafka bất đồng bộ
                producer.sendEvent(event);
                eventCount++;

                if (eventCount % 100 == 0) {
                    log.info("Milestone: Sent {} customer events so far.", eventCount);
                }

                // Nghỉ 100ms để duy trì tốc độ phát sóng ~10 events/giây
                Thread.sleep(EVENT_INTERVAL_MS);

            } catch (InterruptedException e) {
                log.warn("Main loop interrupted. Exiting event generation...");
                Thread.currentThread().interrupt();
                running.set(false);
            } catch (Exception e) {
                log.error("Unexpected error in event generation loop: {}", e.getMessage(), e);
            }
        }

        log.info("Main execution ended. Total events dispatched: {}", eventCount);
    }
}
