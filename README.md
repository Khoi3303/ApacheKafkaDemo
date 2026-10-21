# ⚡ Apache Kafka Video Streaming & Pub/Sub Broadcast Demo
> **Dự án mô phỏng Event Streaming, Video Chunking và Kiến trúc 1 Nguồn phát - Nhiều Nguồn thu với Apache Kafka**

---

## 📌 1. Giới Thiệu Đề Tài
Dự án giải quyết bài toán cốt lõi trong xử lý dữ liệu lớn (Big Data / Media Streaming):
- **Event Streaming:** Truyền dữ liệu liên tục theo thời gian thực thay vì truyền cả file tĩnh dung lượng lớn qua mạng.
- **Data Chunking (Băm nhỏ dữ liệu):** Video bài giảng được chia nhỏ thành các gói (chunks 32KB - 256KB) kèm metadata, checksum và timestamp.
- **Publish-Subscribe (Pub/Sub) Broadcast Architecture:** 1 Producer duy nhất phát dòng sự kiện vào Topic Kafka, sau đó Kafka **tự động broadcast (fan-out)** đồng thời đến **3 Consumer Groups độc lập**:
  1. 🎬 **Consumer Group 1 (`cg-video-streamer`):** Tái ghép các mảng byte nhị phân để phát video liên tục ngay trên trình duyệt (Progressive Video Playback).
  2. 🔬 **Consumer Group 2 (`cg-msg-inspector`):** Soi chi tiết dữ liệu kỹ thuật từng Kafka Message (Partition Key, Hash, Offset, Byte range, Checksum, Hex Preview).
  3. 🧩 **Consumer Group 3 (`cg-reassembly-tracker`):** Giám sát quá trình gom và tái ghép các chunk phân tán từ 3 Partitions theo thời gian thực.

---

## 💡 2. Vì Sao Phải Băm Nhỏ Dữ Liệu (Video Chunking)?

| Vấn đề | Truyền cả file dung lượng lớn (HTTP/SQL BLOB) | Băm nhỏ thành Chunks qua Kafka |
| :--- | :--- | :--- |
| **Độ trễ (Latency)** | Người nghe phải đợi tải xong toàn bộ file mới bắt đầu xem được. | Người nghe xem được ngay lập tức ở giây đầu tiên khi Chunk 0 vừa đến. |
| **Bộ nhớ (Memory)** | Gây tràn bộ nhớ (Out-Of-Memory Heap) tại cả Server, Broker và Client. | Tiêu thụ bộ nhớ cực thấp, truyền theo luồng (Stream buffer). |
| **Độ chịu lỗi (Fault Tolerance)** | Nếu mạng bị rớt khi đang tải 99%, toàn bộ file bị hủy và phải tải lại từ đầu. | Nếu rớt mạng ở chunk nào, Kafka chỉ cần phát lại (retry) đúng chunk đó. |
| **Xử lý song song** | Không thể xử lý song song khi file chưa truyền xong. | Các Consumer có thể vừa xử lý dữ liệu của Chunk 1 trong khi Producer đang truyền Chunk 5. |

---

## 👥 3. Cơ Chế 1 Nguồn Phát - Nhiều Nguồn Thu (Pub/Sub Fan-Out)
- Trong Queue truyền thống (RabbitMQ standard queue), 1 tin nhắn khi được tiêu thụ sẽ bị xóa khỏi hàng đợi.
- Trong **Apache Kafka**:
  - Dữ liệu được lưu trữ trên đĩa cứng dưới dạng **Append-only Commit Log** theo thời gian cấu hình (Retention time).
  - Mỗi bên thu đăng ký một **Consumer Group** riêng biệt.
  - Mỗi Consumer Group lưu trữ một con trỏ **Offset** độc lập trên từng Partition.
  - Khi Producer gửi 1 chunk, **Kafka tự động phát tán (Broadcast) chunk đó tới TẤT CẢ các Consumer Groups**.
  - Nếu một Consumer xử lý chậm hoặc bị ngắt kết nối, **Consumer Lag** của nhóm đó tăng lên, nhưng **KHÔNG hề làm chậm hay nghẽn các Consumer còn lại**!

---

## 🚀 4. Hướng Dẫn Cài Đặt & Chạy Demo

### Cách 1: Chạy Siêu Tốc (Embedded Mode)
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
   - Bấm **"Bắt Đầu Stream"**: Quan sát đồ họa luồng dữ liệu (Canvas) phát tán các chunks vào Kafka Partitions và 3 Consumer Groups.
   - Quan sát Video bài giảng phát trực tiếp, Message Inspector soi từng byte nhị phân, và Reassembly Tracker hiển thị bản đồ ghép chunk.
   - Bấm **"Ngắt Consumer"**: Quan sát **Consumer Lag** tăng lên trong khi các Consumer khác vẫn chạy mượt mà.
   - Bấm **"Tiếp Tục Nhận"**: Quan sát Kafka **Catch-up / Offset Replay** đọc bù toàn bộ dữ liệu bị trễ!

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

## 📂 5. Cấu Trúc Mã Nguồn

```
ApacheKafkaDemo/
├── README.md                      # Hướng dẫn chi tiết kỹ thuật
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
│   ├── styles.css                 # Hệ thống giao diện hiện đại, responsive
│   └── app.js                     # Điều khiển Canvas, WebSocket, Video Player, Inspector
└── scripts/
    ├── download_sample.js         # Script tải video mẫu
    ├── test_demo.js               # Script kiểm thử tự động toàn diện
    └── reset.js                   # Script reset hệ thống
```

---

## 📊 6. Tính Năng Kỹ Thuật Nổi Bật
- [x] Mô phỏng trực quan cơ chế băm nhỏ dữ liệu và truyền phát streaming thời gian thực.
- [x] Minh chứng rõ ràng kiến trúc 1 nguồn phát - nhiều nguồn thu (Pub/Sub Fan-out).
- [x] Mô phỏng và đo lường Consumer Lag, Offset Commit, Catch-up Replay.
- [x] Hỗ trợ kết nối đồng thời In-Memory Engine và Apache Kafka KRaft thật qua Docker.
- [x] Giao diện Dashboard tương tác trực quan, responsive tự động co giãn.
