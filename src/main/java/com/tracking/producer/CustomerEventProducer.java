package com.tracking.producer;

import com.tracking.model.CustomerEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;

/**
 * Kafka Producer chịu trách nhiệm đẩy các sự kiện CustomerEvent vào Apache Kafka topic.
 * <p>
 * Được cấu hình chuẩn Enterprise Data Pipeline:
 * <ul>
 *     <li><b>Key là accId</b>: Đảm bảo toàn bộ tương tác của một khách hàng luôn đi vào cùng 1 partition
 *     để giữ tính thứ tự nghiêm ngặt (Strict Ordering).</li>
 *     <li><b>Idempotent Producer</b>: {@code enable.idempotence = true}, {@code acks = all},
 *     {@code retries = MAX_VALUE} giúp ngăn chặn duplicate message khi xảy ra retry trên mạng.</li>
 *     <li><b>Batching & Throughput</b>: {@code linger.ms = 20} gom các message đến liên tục thành một batch nhỏ,
 *     giảm số lượng round-trip network I/O.</li>
 * </ul>
 */
@Slf4j
public class CustomerEventProducer implements AutoCloseable {

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_TOPIC_NAME = "customer-events-raw";

    private final String topicName;
    private final KafkaProducer<String, String> producer;

    /**
     * Khởi tạo Producer với cấu hình mặc định (localhost:9092 và topic customer-events-raw).
     */
    public CustomerEventProducer() {
        this(DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_TOPIC_NAME);
    }

    /**
     * Khởi tạo Producer tùy biến địa chỉ Kafka broker và topic name.
     *
     * @param bootstrapServers Địa chỉ Kafka cluster (VD: "localhost:9092")
     * @param topicName        Tên topic nhận dữ liệu (VD: "customer-events-raw")
     */
    public CustomerEventProducer(String bootstrapServers, String topicName) {
        this.topicName = topicName;

        Properties props = new Properties();

        // 1. Cấu hình địa chỉ Cluster & Serializers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Cấu hình Độ tin cậy cao (High Reliability & Exactly-Once Semantics ở Producer level)
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // 3. Cấu hình Tối ưu Throughput (Batching)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        this.producer = new KafkaProducer<>(props);
        log.info("Kafka CustomerEventProducer initialized successfully for topic: '{}' at: '{}'",
                topicName, bootstrapServers);
    }

    /**
     * Gửi một sự kiện CustomerEvent bất đồng bộ (Asynchronous) đến Kafka.
     * <p>
     * <b>BẮT BUỘC:</b> Sử dụng {@code event.getAccId()} làm Message Key để Kafka thực hiện
     * Murmur2 Hash Partitioning, bảo đảm tính tuần tự (Ordering) theo từng Account.
     *
     * @param event Đối tượng CustomerEvent cần phát hành
     */
    public void sendEvent(CustomerEvent event) {
        if (event == null) {
            log.warn("Attempted to send null CustomerEvent, ignoring.");
            return;
        }

        String key = event.getAccId();
        String jsonValue = event.toJsonString();
        String eventType = event.getEventType();

        ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName, key, jsonValue);

        this.producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    log.info("Produced Event -> Key: [{}], Type: [{}], Partition: [{}], Offset: [{}]",
                            key, eventType, metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to produce Event -> Key: [{}], Type: [{}] to topic [{}]. Error: {}",
                            key, eventType, topicName, exception.getMessage(), exception);
                }
            }
        });
    }

    /**
     * Xả toàn bộ buffer (flush) và đóng kết nối Kafka Producer an toàn (Graceful Shutdown).
     */
    @Override
    public void close() {
        log.info("Closing Kafka CustomerEventProducer...");
        try {
            this.producer.flush();
            this.producer.close(Duration.ofSeconds(5));
            log.info("Kafka CustomerEventProducer closed gracefully.");
        } catch (Exception e) {
            log.error("Error while closing Kafka Producer: {}", e.getMessage(), e);
        }
    }
}
