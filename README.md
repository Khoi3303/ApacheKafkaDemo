# ⚡ Apache Kafka — Video Streaming & Pub/Sub Broadcast Demo

> Minh hoạ **Event Streaming thời gian thực** với **Apache Kafka KRaft** (không cần ZooKeeper):
> 1 Producer băm nhỏ video → Kafka Broker (3 Partitions) → Fan-out đồng thời đến 3 Consumer Groups độc lập.

---

## 🏗️ Kiến Trúc Hệ Thống

```
Frontend (Web Dashboard)
      |
      | HTTP / WebSocket
      v
Backend (Express)
      |
   Video Chunker (64 KB/chunk)
      |
      v
KafkaJS Producer
      |
      v
APACHE KAFKA (KRaft)
Topic: stanford-lecture-stream (3 Partitions: P0, P1, P2)
                   ┌──────────┬──────────┐
                   │   P 0    │   P 1    │   P 2    │
                   └────┬─────┴────┬─────┴────┬─────┘
                        │          │          │
         ┌──────────────┴──────────┼──────────┴──────────────┐
         │ (Fan-out: Mỗi Consumer Group đọc độc lập toàn bộ Topic)  │
         ▼                         ▼                         ▼
   Consumer Group 1          Consumer Group 2          Consumer Group 3
  cg-video-streamer         cg-msg-inspector       cg-reassembly-tracker
(Đọc P0, P1, P2 độc lập)   (Đọc P0, P1, P2 độc lập)  (Đọc P0, P1, P2 độc lập)
         │                         │                         │
         ▼                         ▼                         ▼
   Video Player                Inspector             Reassembly Tracker
 (Tái ghép phát video)    (Soi Offset, Key, CRC)    (Bản đồ phân bố Chunk)
```

> **Lưu ý kiến trúc:** Các Consumer Group hoạt động hoàn toàn độc lập với nhau. Mỗi group tự quản lý committed offset riêng trên từng partition của Topic. Frontend không kết nối trực tiếp với Kafka mà nhận dữ liệu đẩy về qua WebSocket từ Backend.

---

## 🚀 Chạy Demo

### Yêu cầu
- Docker Desktop đang chạy
- Node.js 18+

### Bước 1 — Khởi động Apache Kafka

```bash
docker compose up -d
```

Kafka KRaft broker khởi động tại localhost:9092.
Kafka UI tại http://localhost:8080.

Lần đầu chạy cần tải image (~2 phút).
Kafka sẵn sàng khi healthcheck pass (xanh trong Docker Desktop).

### Bước 2 — Chạy ứng dụng

```bash
npm install
npm start
```

Kết nối thành công sẽ thấy trong terminal:

```
====================================================
 Apache Kafka Video Streaming Demo
 Server: http://localhost:3000
 Kafka: REAL Apache Kafka
 Topic: stanford-lecture-stream
 Consumer Groups: 3
====================================================
Connected to REAL Apache Kafka
Consumer connected: cg-video-streamer
Consumer connected: cg-msg-inspector
Consumer connected: cg-reassembly-tracker
```

### Bước 3 — Mở 2 cửa sổ demo

- Demo App: http://localhost:3000
- Kafka UI:  http://localhost:8080

---

## 🎬 Kịch Bản Demo

### 1. Băm nhỏ video vào Kafka
Bấm "Bắt Đầu Stream" → Producer chia video thành các chunks 64 KB.
Mỗi chunk là 1 Kafka message riêng biệt với:
- Binary video data (raw bytes) làm Kafka value
- Metadata làm Kafka headers: chunk-index, total-chunks, start-byte, end-byte, checksum

### 2. Kafka phân phối vào 3 Partitions
Quan sát Partition Log — Kafka phân phối chunks vào Partition 0, 1, 2.
Xác nhận trên Kafka UI → Topics → stanford-lecture-stream.

### 3. Fan-out — 1 nguồn phát, 3 Consumer Groups nhận độc lập
Cùng 1 chunk xuất hiện đồng thời ở cả 3 consumer groups.
Mỗi group xử lý theo cách riêng, offset hoàn toàn độc lập.

### 4. Demo Consumer Lag
1. Bấm "Ngắt Inspector" (Consumer 2)
2. Bấm "Bắt Đầu Stream" lần nữa
3. Quan sát TOTAL CONSUMER LAG tăng — Consumer 1 và 3 vẫn chạy bình thường
4. Bấm "Kết nối lại" → Kafka Catch-up Replay từ committed offset

---

## 🔍 Chứng Minh Kafka Thật (CLI)

Chạy trực tiếp vào Docker container để xác nhận dữ liệu thật từ Kafka broker:

```bash
# Xem topic tồn tại trong broker
docker exec kafka-kraft-broker \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list

# Xem offsets thực tế (tăng sau mỗi lần stream)
docker exec kafka-kraft-broker \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:29092 \
  --topic stanford-lecture-stream

# Xem Consumer Groups + Lag thực tế
docker exec kafka-kraft-broker \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 \
  --describe --all-groups
```

Tip: Chạy kafka-get-offsets.sh trước khi stream (offset = 0),
sau đó stream xong chạy lại — offset tăng đúng bằng số chunks.
Đây là bằng chứng không thể làm giả bằng UI.

---

## 📂 Cấu Trúc Mã Nguồn

```
ApacheKafkaDemo/
├── README.md
├── docker-compose.yml          # Apache Kafka KRaft + Kafka UI
├── Dockerfile                  # Đóng gói Node.js app
├── package.json
├── server/
│   ├── server.js               # Express + WebSocket + 3 Consumer Groups
│   ├── kafka-engine.js         # KafkaJS: Producer, Consumer, Admin
│   ├── video-chunker.js        # Băm video thành Kafka binary messages
│   └── data/
│       └── stanford_lecture_sample.mp4
└── client/
    ├── index.html              # Dashboard UI
    ├── styles.css
    └── app.js                  # WebSocket client, Canvas, Video Player
```

---

## 📊 Tính Năng

- Apache Kafka thật — KRaft mode, không ZooKeeper, chạy trong Docker
- Binary video chunking — Raw bytes làm Kafka value, metadata làm headers
- 3 Consumer Groups độc lập — Mỗi group có offset riêng, fan-out thật
- Consumer Lag thật — Đọc từ admin.fetchOffsets() của Kafka broker
- Catch-up Replay — Consumer kết nối lại đọc bù từ committed offset
- Kafka UI tích hợp — Xem topic, partition, consumer group trực tiếp
- CLI verification — Xác minh bằng Kafka shell scripts trong Docker
