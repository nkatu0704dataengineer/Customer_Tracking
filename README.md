# Customer Tracking Pipeline (Task 1: Project Setup & Event Model)

Dự án Java Maven xử lý pipeline theo dõi sự kiện người dùng thời gian thực với **Java 17**, **Apache Kafka Client 3.x**, **Jackson**, **SLF4J/Logback**, và **Lombok**.

---

## 📁 Cấu trúc thư mục dự án

```text
customer-tracking-pipeline/
├── pom.xml
├── README.md
└── src/
    ├── main/
    │   ├── java/
    │   │   └── com/
    │   │       └── tracking/
    │   │           ├── CustomerTrackingApp.java        # Main class chạy thử nghiệm
    │   │           └── model/
    │   │               └── CustomerEvent.java          # Model Class cốt lõi của Pipeline
    │   └── resources/
    │       └── logback.xml                             # Cấu hình log Console định dạng chuẩn
    └── test/
        └── java/
            └── com/
                └── tracking/
                    └── model/
                        └── CustomerEventTest.java      # Unit test serialize & deserialize
```

---

## ⚙️ Chi tiết thành phần

### 1. `pom.xml`
- **Java Version**: 17 (`maven.compiler.source` & `maven.compiler.target = 17`)
- **Apache Kafka Clients**: `org.apache.kafka:kafka-clients:3.7.0` (Tương thích 3.6.0+)
- **Jackson Databind**: `com.fasterxml.jackson.core:jackson-databind:2.17.0` cùng `jackson-datatype-jsr310`
- **Logging**: `org.slf4j:slf4j-api:2.0.12` và `ch.qos.logback:logback-classic:1.5.3`
- **Lombok**: `org.projectlombok:lombok:1.18.32` được cấu hình qua `maven-compiler-plugin` annotation processor paths

### 2. Model Class: `com.tracking.model.CustomerEvent`
- Thiết kế theo tư duy **Senior Data / Platform Engineer**:
  - `customerId`: Dùng làm **Kafka Message Key** để Kafka phân vùng (Partition Hashing), bảo đảm toàn bộ event của cùng một customer luôn đi vào cùng partition và giữ đúng thứ tự thời gian (**Strict Ordering per Customer**).
  - `eventId`: UUID ngẫu nhiên duy nhất cho mỗi event, hỗ trợ **Deduplication / Idempotent Processing** tại downstream (Kafka Streams, Flink, Spark).
  - `timestamp`: Đánh dấu **Event Time** (thời gian thực tế người dùng tương tác), phục vụ chuẩn xác các thuật toán Event-time Windowing.
  - `metadata`: `Map<String, Object>` cho phép tiến hóa schema linh hoạt (**Schema Evolution**) khi cần gửi kèm các thông tin tùy biến (price, product_id, session,...).
  - Cung cấp sẵn các hằng số: `EventTypes` (`CLICK_PRODUCT`, `VIEW_ITEM`, `ADD_TO_CART`, `CHECKOUT`) và `DeviceTypes` (`MOBILE`, `DESKTOP`, `TABLET`).
  - Thread-safe `toJsonString()` và `fromJson(String json)` qua singleton Jackson `ObjectMapper`.

### 3. Logback Console Appender (`logback.xml`)
- In log định dạng màu (Pattern Layout) rõ ràng: thời gian chi tiết ms, thread, log level, class name và message.
- Tự động hạ log level của thư viện Kafka xuống `WARN` để tránh flood log kết nối nội bộ trong lúc chạy.

---

## 🚀 Cách chạy và kiểm thử

1. **Mở dự án trong IDE (IntelliJ IDEA / VS Code)**:
   - Mở thư mục `customer-tracking-pipeline` dưới dạng Maven Project.
   - Bật hỗ trợ Annotation Processing (đối với Lombok).
2. **Chạy thử qua Class App**:
   - Chạy hàm `main()` tại [CustomerTrackingApp.java](file:///C:/Users/Admin/.gemini/antigravity-ide/scratch/customer-tracking-pipeline/src/main/java/com/tracking/CustomerTrackingApp.java).
3. **Chạy Unit Test**:
   - Chạy test suite [CustomerEventTest.java](file:///C:/Users/Admin/.gemini/antigravity-ide/scratch/customer-tracking-pipeline/src/test/java/com/tracking/model/CustomerEventTest.java).
