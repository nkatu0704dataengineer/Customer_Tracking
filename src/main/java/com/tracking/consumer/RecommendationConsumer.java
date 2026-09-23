package com.tracking.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracking.model.CustomerEvent;
import com.tracking.model.Product;
import com.tracking.repository.ProductCatalogRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RecommendationConsumer implements Runnable {
    private final KafkaConsumer<String, String> consumer;
    private final ProductCatalogRepository repository;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private static final String TOPIC = "customer-events-raw";
    private static final int RECOMMENDATION_LIMIT = 5;

    public RecommendationConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        this.consumer = new KafkaConsumer<>(props);
        this.repository = new ProductCatalogRepository();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            log.info("Subscribed to topic: {}", TOPIC);

            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
                
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    log.debug("Committed offset synchronously");
                }
            }
        } catch (Exception e) {
            log.error("Error in consumer", e);
        } finally {
            consumer.close();
            log.info("Kafka consumer closed gracefully");
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            CustomerEvent event = objectMapper.readValue(record.value(), CustomerEvent.class);
            String eventType = event.getEventType();
            
            if (eventType != null && 
               (eventType.equals(CustomerEvent.EventTypes.VIEW) || 
                eventType.equals(CustomerEvent.EventTypes.CLICK) || 
                eventType.equals(CustomerEvent.EventTypes.SEARCH))) {
                
                String productId = event.getProductId();
                if (productId != null && !productId.isEmpty()) {
                    List<Product> recommendations = repository.getRecommendations(productId, RECOMMENDATION_LIMIT);
                    if (!recommendations.isEmpty()) {
                        log.info("Recommendations for accId: {}, session: {}, action: {} on product: {} -> {}", 
                                event.getAccId(), event.getSessionId(), eventType, productId, recommendations);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to process record: {}", record.value(), e);
        }
    }

    public void shutdown() {
        running.set(false);
    }
}
