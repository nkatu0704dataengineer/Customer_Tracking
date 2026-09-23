package com.tracking.generator;

import com.tracking.model.CustomerEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bộ sinh dữ liệu giả lập (Synthetic Data Generator) cho Customer Tracking Events.
 * <p>
 * Tính năng nổi bật:
 * <ul>
 *     <li><b>100 Accounts:</b> Dải Account ID cố định "ACC00000001" -> "ACC00000100".</li>
 *     <li><b>200 Products:</b> Dải Product ID cố định "PR001" -> "PR200".</li>
 *     <li><b>Thuật toán Stateful Sessionization:</b> Quản lý trạng thái phiên làm việc (Session) của từng
 *     khách hàng dựa trên khoảng thời gian không hoạt động (Session Inactivity Timeout).
 *     Nếu thời gian gián đoạn giữa 2 hành vi {@code timeDeltaMinutes > 15 phút}, hệ thống sẽ tự động
 *     tạo một phiên mới (Session ID mới dạng SS%08d).</li>
 *     <li><b>Thread-safe:</b> Sử dụng {@link ConcurrentHashMap} và đồng bộ trên từng {@link AccountState}.</li>
 * </ul>
 */
@Slf4j
public class CustomerEventGenerator {

    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    private static final int SESSION_TIMEOUT_MINUTES = 15;

    // Danh sách 100 Account ID (ACC00000001 -> ACC00000100)
    private final List<String> accountIds = new ArrayList<>(100);

    // Danh sách 200 Product ID (PR001 -> PR200)
    private final List<String> productIds = new ArrayList<>(200);

    // Danh sách thiết bị người dùng
    private static final List<String> DEVICE_TYPES = List.of("Mobile", "Desktop", "Tablet");

    // Danh sách toàn bộ các loại sự kiện chuẩn
    private static final List<String> ALL_EVENT_TYPES = List.of(
            CustomerEvent.EventTypes.VIEW,
            CustomerEvent.EventTypes.ADD_TO_CART,
            CustomerEvent.EventTypes.SEARCH,
            CustomerEvent.EventTypes.WISHLIST,
            CustomerEvent.EventTypes.CLICK,
            CustomerEvent.EventTypes.CHECKOUT,
            CustomerEvent.EventTypes.REMOVE_FROM_CART,
            CustomerEvent.EventTypes.PURCHASE
    );

    // Bảng lưu trữ trạng thái Session của từng Account (Thread-safe)
    private final Map<String, AccountState> accountStateMap = new ConcurrentHashMap<>();

    /**
     * Lớp nội bộ theo dõi trạng thái phiên (Session) của một tài khoản cụ thể.
     */
    @Getter
    public static class AccountState {
        private int currentSessionNumber;
        private ZonedDateTime lastEventTime;

        public AccountState(int initialSessionNumber, ZonedDateTime initialEventTime) {
            this.currentSessionNumber = initialSessionNumber;
            this.lastEventTime = initialEventTime;
        }

        /**
         * Cập nhật phiên làm việc của tài khoản dựa trên độ trễ giữa 2 sự kiện liên tiếp.
         *
         * @param timeDeltaMinutes Số phút chênh lệch kể từ sự kiện trước đó
         * @return Session ID dạng chuỗi (VD: "SS00000001")
         */
        public synchronized SessionInfo advanceSession(int timeDeltaMinutes) {
            ZonedDateTime newEventTime = this.lastEventTime.plusMinutes(timeDeltaMinutes);

            if (timeDeltaMinutes > SESSION_TIMEOUT_MINUTES) {
                this.currentSessionNumber++;
            }

            this.lastEventTime = newEventTime;
            String sessionId = String.format("SS%08d", this.currentSessionNumber);
            return new SessionInfo(sessionId, newEventTime);
        }
    }

    /**
     * DTO lưu trữ thông tin session sau khi tính toán.
     */
    public record SessionInfo(String sessionId, ZonedDateTime eventTime) {}

    public CustomerEventGenerator() {
        initDataPool();
    }

    /**
     * Khởi tạo pool 100 Accounts và 200 Products.
     */
    private void initDataPool() {
        // 1. Sinh 100 Account IDs: ACC00000001 -> ACC00000100
        for (int i = 1; i <= 100; i++) {
            accountIds.add(String.format("ACC%08d", i));
        }

        // 2. Sinh 200 Product IDs: PR001 -> PR200
        for (int i = 1; i <= 200; i++) {
            productIds.add(String.format("PR%03d", i));
        }

        log.info("Initialized CustomerEventGenerator: {} accounts (ACC00000001-ACC00000100), {} products (PR001-PR200).",
                accountIds.size(), productIds.size());
    }

    /**
     * Sinh một sự kiện CustomerEvent theo thuật toán Stateful Sessionization.
     *
     * @return CustomerEvent hoàn chỉnh với đầy đủ thông tin chuẩn hóa
     */
    public CustomerEvent generateEvent() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // 1. Chọn ngẫu nhiên một accId từ 100 accounts
        String accId = accountIds.get(random.nextInt(accountIds.size()));

        // 2. Thuật toán Sessionization: Lấy hoặc khởi tạo trạng thái tài khoản
        SessionInfo sessionInfo;
        AccountState state = accountStateMap.get(accId);

        if (state == null) {
            // Lần đầu xuất hiện: Khởi tạo session 1 tại thời điểm hiện tại
            ZonedDateTime now = ZonedDateTime.now();
            AccountState newState = new AccountState(1, now);
            AccountState existing = accountStateMap.putIfAbsent(accId, newState);
            if (existing == null) {
                sessionInfo = new SessionInfo(String.format("SS%08d", 1), now);
            } else {
                int timeDelta = random.nextInt(1, 31);
                sessionInfo = existing.advanceSession(timeDelta);
            }
        } else {
            // Các lần tiếp theo: Sinh chênh lệch 1 đến 30 phút
            int timeDeltaMinutes = random.nextInt(1, 31);
            sessionInfo = state.advanceSession(timeDeltaMinutes);
        }

        String sessionId = sessionInfo.sessionId();
        ZonedDateTime eventTimeObj = sessionInfo.eventTime();

        // 3. Chọn ngẫu nhiên eventType từ EventTypes
        String eventType = ALL_EVENT_TYPES.get(random.nextInt(ALL_EVENT_TYPES.size()));

        // 4. Chọn ngẫu nhiên productId từ 200 products
        String productId = productIds.get(random.nextInt(productIds.size()));

        // 5. Chọn ngẫu nhiên deviceType
        String deviceType = DEVICE_TYPES.get(random.nextInt(DEVICE_TYPES.size()));

        // 6. Sinh timeOnPageMinutes ngẫu nhiên từ 1 đến 60
        int timeOnPageMinutes = random.nextInt(1, 61);

        // 7. Định dạng thời gian theo chuẩn ISO-8601
        String formattedTime = eventTimeObj.format(ISO_FORMATTER);

        return CustomerEvent.builder()
                .accId(accId)
                .sessionId(sessionId)
                .eventTime(formattedTime)
                .eventType(eventType)
                .productId(productId)
                .deviceType(deviceType)
                .timeOnPageMinutes(timeOnPageMinutes)
                .createdAt(formattedTime)
                .updatedAt(formattedTime)
                .build();
    }
}
