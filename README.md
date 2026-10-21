# ⚡ Apache Kafka Video Streaming & Pub/Sub Broadcast Demo
> **Dự án mô phỏng Event Streaming, Video Chunking và Kiến trúc 1 Nguồn phát - Nhiều Nguồn thu với Apache Kafka**  
> *Được thiết kế chuẩn theo yêu cầu và đề cương hướng dẫn thuyết trình (20%) của Giảng viên.*

---

## 📌 1. Giới Thiệu Đề Tài (WHAT)
Dự án giải quyết bài toán cốt lõi trong xử lý dữ liệu lớn (Big Data / Media Streaming):
- **Event Streaming:** Truyền dữ liệu liên tục theo thời gian thực thay vì truyền cả file tĩnh 1GB qua mạng.
- **Data Chunking (Băm nhỏ dữ liệu):** Video bài giảng (Stanford CS229 Lecture) được chia nhỏ thành các gói (chunks 32KB - 256KB) kèm metadata, checksum và timestamp.
- **Publish-Subscribe (Pub/Sub) Broadcast Architecture:** 1 Producer duy nhất phát dòng sự kiện vào Topic Kafka, sau đó Kafka **tự động broadcast (fan-out)** đồng thời đến **3 Consumer Groups độc lập**:
  1. 🎬 **Consumer Group 1 (`cg-video-streamer`):** Tái ghép các mảng byte nhị phân để phát video liên tục ngay trên trình duyệt (Progressive Video Playback).
  2. 🤖 **Consumer Group 2 (`cg-ai-transcribe`):** Chạy song song mô hình AI nhận diện giọng nói (Speech-to-Text) tạo phụ đề bài giảng thời gian thực.
  3. 🗄️ **Consumer Group 3 (`cg-db-archiver`):** Lưu trữ metadata và lịch sử các chunk vào hệ thống CSDL SQL (Async Database Write Buffer).

---

## 🎯 2. Trả Lời Câu Hỏi Của Thầy: "Kafka Nên Nằm Ở Đâu Trong Source Code?"

Đây là câu hỏi kiến trúc kinh điển mà Thầy muốn nhóm làm rõ: **Kafka nằm giữa Frontend - Backend hay giữa Backend - SQL?**

```
                     KIẾN TRÚC TỔNG THỂ HỆ THỐNG
                     
   [ Trình duyệt / Mobile App ] (Frontend)
                │
                │  HTTP POST / WebSockets / WebRTC (Không dùng TCP 9092 thô)
                ▼
   ┌──────────────────────────────────────────────────┐
   │       BACKEND API GATEWAY & INGESTION SERVICE     │ (Backend Producer)
   │  - Xác thực (Auth) & Phân quyền                  │
   │  - Băm nhỏ video thành các chunks 64KB           │
   │  - Gắn Checksum & Metadata                       │
   └──────────────────────┬───────────────────────────┘
                          │
                          │  Kafka Producer API (TCP Binary Protocol)
                          ▼
   ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
   ┃                APACHE KAFKA CLUSTER              ┃
   ┃       Topic: 'stanford-lecture-stream'           ┃
   ┃                                                  ┃
   ┃   Partition 0:  [0] [1] [2] [3] ...              ┃
   ┃   Partition 1:  [0] [1] [2] ...                  ┃
   ┃   Partition 2:  [0] [1] [2] ...                  ┃
   ┃   (Append-Only Commit Logs trên Đĩa Cứng)        ┃
   ┗━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━┛
                ┃                 ┃                 ┃
   (Fan-Out)    ┃                 ┃                 ┃ (Pub/Sub Broadcast)
                ▼                 ▼                 ▼
     ┌──────────────────┐ ┌───────────────┐ ┌──────────────────┐
     │ Consumer Group 1 │ │Consumer Group 2│ │ Consumer Group 3 │
     │  (Live Player)   │ │ (AI Speech)   │ │  (DB Archiver)   │
     └─────────┬────────┘ └───────┬───────┘ └────────┬─────────┘
               │                  │                  │
               ▼                  ▼                  ▼
       [HTML5 Video]        [Phụ đề AI]       [SQL Database]
                                          (Bảo vệ DB chống sập)
```

### Chi tiết 3 vị trí trong kiến trúc:

#### 1. Đặt Kafka giữa Frontend và Backend? ❌ (Anti-pattern trong thực tế)
- **Lý do KHÔNG NÊN:**
  - Trình duyệt web chạy trên môi trường người dùng không tin cậy. Nếu kết nối trực tiếp cổng 9092 của Kafka, bạn phải mở public broker ra Internet, nguy cơ bị DDoS, chiếm quyền điều khiển và spam tốn tài nguyên.
  - Trình duyệt chỉ hỗ trợ HTTP/HTTPS, WebSocket, WebRTC; không hỗ trợ native TCP socket của Kafka protocol.
  - Hàng triệu client mở kết nối trực tiếp sẽ làm cạn kiệt connection pool của Kafka Broker.
- **Giải pháp chuẩn:** Frontend kết nối với **API Gateway / Backend Ingestion Service** qua HTTP/WebSocket, sau đó Backend đóng vai trò là **Kafka Producer** để đẩy tin nhắn vào Broker.

#### 2. Đặt Kafka giữa Backend và SQL Database? ✅ (Rất phổ biến & Chuẩn thiết kế)
- **Kỹ thuật Async Write Buffering (Chống sập Database):**
  - Khi hàng triệu bản ghi / chunks video đổ về trong 1 giây, nếu Backend ghi thẳng vào CSDL SQL với các transaction ACID và index nặng, database sẽ bị nghẽn (connection pool exhaustion) và sập (crash).
  - Đặt Kafka ở giữa làm **vùng đệm (buffer)**: Producer đẩy dữ liệu vào Kafka cực nhanh (hàng triệu ops/sec).
  - Consumer Group của Database sẽ kéo dữ liệu từ Kafka và ghi vào SQL theo đợt (**Batch Insert**) với tốc độ mà database có thể chịu được mà không làm chậm người dùng.
- **Kỹ thuật CDC (Change Data Capture):** Sử dụng Kafka Connect / Debezium để bắt các thay đổi từ database write-ahead log (WAL) và đẩy ngược vào Kafka topic để đồng bộ sang ElasticSearch / Redis / Data Lake.

#### 3. Vị trí cốt lõi nhất của Kafka trong hệ thống: 🏆
- Kafka đóng vai trò là **Distributed Event Backbone (Xương sống sự kiện phân tán)** kết nối độc lập giữa các Microservices ở tầng Backend.

---

## 💡 3. Vì Sao Phải Băm Nhỏ Dữ Liệu (Video Chunking)? (WHY)

| Vấn đề | Truyền cả file 1GB nguyên khối (HTTP/SQL BLOB) | Băm nhỏ thành Chunks 64KB qua Kafka |
| :--- | :--- | :--- |
| **Độ trễ (Latency)** | Người nghe phải đợi tải xong 1GB mới bắt đầu xem được. | Người nghe xem được ngay lập tức ở giây đầu tiên khi Chunk 0 vừa đến. |
| **Bộ nhớ (Memory)** | Gây tràn bộ nhớ (Out-Of-Memory JVM Heap) tại cả Server, Broker và Client. | Tiêu thụ bộ nhớ cực thấp, truyền theo luồng (Stream buffer). |
| **Độ chịu lỗi (Fault Tolerance)** | Nếu mạng bị rớt khi đang tải 99%, toàn bộ 1GB bị hủy và phải tải lại từ đầu. | Nếu rớt mạng ở chunk nào, Kafka chỉ cần phát lại (retry) đúng chunk đó. |
| **Xử lý song song** | Không thể xử lý song song khi file chưa truyền xong. | Mô hình AI có thể vừa dịch phụ đề cho Chunk 1 trong khi Producer đang truyền Chunk 5. |

---

## 👥 4. Cơ Chế 1 Nguồn Phát - Nhiều Nguồn Thu (Pub/Sub Fan-Out)
- Trong Queue truyền thống (RabbitMQ standard queue), 1 tin nhắn khi được tiêu thụ sẽ bị xóa khỏi hàng đợi.
- Trong **Apache Kafka**:
  - Dữ liệu được lưu trữ trên đĩa cứng dưới dạng **Append-only Commit Log** theo thời gian cấu hình (Retention time).
  - Mỗi bên thu đăng ký một **Consumer Group** riêng biệt.
  - Mỗi Consumer Group lưu trữ một con trỏ **Offset** độc lập trên từng Partition.
  - Khi Producer gửi 1 chunk, **Kafka tự động phát tán (Broadcast) chunk đó tới TẤT CẢ các Consumer Groups**.
  - Nếu Consumer 2 (AI) xử lý chậm, **Consumer Lag** của nhóm 2 tăng lên, nhưng **KHÔNG hề làm chậm hay nghẽn Consumer 1 (Video Player)**!

---

## 🚀 5. Hướng Dẫn Thực Hành Từng Bước (HOW - Chạy Ngay Tại Lớp)

### Cách 1: Chạy Siêu Tốc (Khuyến nghị khi demo trên lớp)
Không cần cài đặt Docker hay Zookeeper phức tạp, dự án đã tích hợp sẵn **High-Fidelity Kafka Engine** tương thích chuẩn:

1. **Cài đặt thư viện:**
   ```bash
   npm install
   ```

2. **Khởi chạy Demo Server:**
   ```bash
   npm start
   ```

3. **Mở trình duyệt:**
   Truy cập: [http://localhost:3000](http://localhost:3000)

4. **Kịch bản thao tác trên giao diện:**
   - Bấm **"▶ Bắt Đầu Stream (Produce)"**: Quan sát đồ họa luồng dữ liệu (Canvas) phát tán các chunks vào Kafka Partitions và 3 Consumer Groups.
   - Quan sát Video bài giảng Stanford phát trực tiếp, AI hiển thị phụ đề thời gian thực, SQL ghi log vào bảng.
   - Bấm **"⏸ Tạm dừng Nhận"** tại Consumer 2 (AI): Quan sát **Consumer Lag** tăng lên trong khi Consumer 1 (Video) vẫn chạy mượt mà.
   - Bấm **"▶ Tiếp tục Nhận"** tại Consumer 2: Quan sát Kafka **Catch-up / Offset Replay** đọc bù toàn bộ dữ liệu bị trễ!
   - Chuyển qua các Tab: **"Kafka Nằm Ở Đâu?"**, **"Kafka Internals"**, **"SQL Storage"**, và **"Hướng Dẫn Thuyết Trình"**.

---

### Cách 2: Chạy với Apache Kafka Thật (KRaft Mode - Docker)
Dành cho các bạn muốn trải nghiệm cụm Kafka phân tán thực tế:

1. **Khởi chạy cụm Kafka KRaft (không cần ZooKeeper):**
   ```bash
   docker compose up -d
   ```

2. **Kiểm tra Kafka UI:**
   Mở trình duyệt: [http://localhost:8080](http://localhost:8080) để xem Topic `stanford-lecture-stream`, các Partitions và Consumer Groups thực tế.

3. **Chạy ứng dụng Bridge:**
   ```bash
   npm start
   ```

---

## 📂 6. Cấu Trúc Mã Nguồn

```
ApacheKafkaDemo/
├── README.md                      # Hướng dẫn chi tiết kỹ thuật
├── presentation_guide.md          # Kịch bản thuyết trình 20% từng slide cho nhóm
├── docker-compose.yml             # Cấu hình cụm Kafka KRaft thực tế
├── package.json                   # Thư viện phụ thuộc (express, ws, kafkajs)
├── server/
│   ├── server.js                  # Express + WebSocket + Kafka Consumer Dispatcher
│   ├── kafka-engine.js            # Lõi Kafka Broker: Partitions, Offsets, Consumer Groups, Lag
│   ├── video-chunker.js           # Bộ băm nhỏ video bài giảng thành các sự kiện Kafka
│   └── data/
│       └── stanford_lecture_sample.mp4  # Video bài giảng mẫu chuẩn
├── client/
│   ├── index.html                 # Giao diện Dashboard tương tác trực quan
│   ├── styles.css                 # Hệ thống giao diện Dark Mode Glassmorphism cao cấp
│   └── app.js                     # Điều khiển Canvas, WebSocket, Video Player, AI Terminal
└── scripts/
    ├── download_sample.js         # Script tải video mẫu
    ├── test_demo.js               # Script kiểm thử tự động toàn diện
    └── reset.js                   # Script reset hệ thống
```

---

## 👨‍🏫 7. Tác Quyền & Đánh Giá
- **Môn học:** Hệ Phân Tán / Kiến Trúc Phần Mềm & Dữ Liệu Lớn.
- **Tiêu chí đáp ứng:**
  - [x] Làm rõ WHAT, WHY, HOW, WHEN.
  - [x] Demo trực quan băm nhỏ dữ liệu và truyền phát streaming.
  - [x] Minh chứng rõ ràng kiến trúc 1 nguồn phát - nhiều nguồn thu.
  - [x] Phân tích chuẩn xác vị trí Kafka trong source code (Frontend vs Backend vs SQL).
  - [x] Hướng dẫn thực hành từng bước, chạy lại được ngay tại lớp.
