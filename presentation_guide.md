# 🎤 KỊCH BẢN THUYẾT TRÌNH BẢO VỆ ĐỀ TÀI APACHE KAFKA
> **Tài liệu chuẩn bị bảo vệ:** Bám sát tuyệt đối kiến trúc code thực tế, giải thích chính xác cơ chế Kafka (Topic, Partition, Offset, Consumer Group, Lag, Fan-out) và bộ câu hỏi vấn đáp phản biện.

---

## 🌟 3 CÂU "NẰM LÒNG" PHẢI NÓI CHUẨN KHI BẢO VỆ

1. **Về vai trò của Producer & Kafka:**
   > *"Producer splits the video into chunks. Kafka does not split the video; Kafka stores, partitions, and distributes the chunk messages."*  
   > *(Producer của ứng dụng chủ động băm nhỏ video thành các chunk. Bản thân Kafka không tự băm video; Kafka đóng vai trò lưu trữ, phân vùng và phân phối các thông điệp chunk đó).*

2. **Về cơ chế Fan-out & Consumer Group:**
   > *"Each consumer group has its own offset, so multiple services can independently consume the same event stream."*  
   > *(Mỗi Consumer Group sở hữu con trỏ committed offset hoàn toàn độc lập, nhờ đó nhiều dịch vụ khác nhau có thể cùng đọc và xử lý một luồng dữ liệu mà không gây cản trở hay phụ thuộc lẫn nhau).*

3. **Về thứ tự dữ liệu (Ordering) và tái ghép (Reassembly):**
   > *"Kafka guarantees ordering within a partition, while the chunk index allows the application to reconstruct the original video order across partitions."*  
   > *(Kafka đảm bảo thứ tự tuần tự tuyệt đối bên trong từng partition, còn trường chunk-index trong metadata giúp tầng ứng dụng tái ghép lại chính xác video gốc bất kể các chunk được phân tán vào nhiều partition).*

---

## ⏱️ PHÂN BỔ THỜI GIAN THUYẾT TRÌNH (TỔNG: 10 - 15 PHÚT)
- **Phần 1: Khía cạnh lý thuyết & Kiến trúc (3 - 4 phút):** Bản chất Kafka, lý do băm nhỏ dữ liệu, vị trí của Kafka trong hệ thống.
- **Phần 2: Demo thực hành trực quan & Bằng chứng CLI (5 - 7 phút):** Thao tác trên Web Dashboard, quan sát Fan-out 3 Consumer Groups, mô phỏng Consumer Lag & Catch-up Replay, đối chiếu Kafka UI và CLI.
- **Phần 3: Vấn đáp phản biện (3 - 5 phút):** Bộ câu hỏi chuyên sâu của Thầy về ordering, scale 5GB, partition key, consumer group topology.

---

## 📑 PHẦN 1: NỘI DUNG LÝ THUYẾT & KIẾN TRÚC

### 1. Bản chất của Apache Kafka
- **Apache Kafka** là nền tảng truyền phát sự kiện phân tán (Distributed Event Streaming Platform), vận hành theo mô hình **Distributed Append-only Commit Log**.
- **Điểm khác biệt cốt lõi với Message Queue truyền thống (như RabbitMQ):**
  - Message Queue truyền thống: Message sau khi consumer đọc xong sẽ bị xóa khỏi queue.
  - Apache Kafka: Message được lưu trữ bền vững trên đĩa cứng theo thời gian (Retention Period). Các Consumer Groups khác nhau tự quản lý con trỏ **Offset** của riêng mình, có thể đọc độc lập và phát lại (Replay) dữ liệu khi cần.

---

### 2. Vì sao phải băm nhỏ dữ liệu video lớn thành các Chunk 64KB?
Nhóm làm rõ 4 lý do kỹ thuật:
1. **Bảo vệ bộ nhớ Broker & Consumer (Chống OOM Crash):** Kafka được thiết kế và tối ưu cho các message từ vài KB đến 1MB. Nếu gửi nguyên tệp lớn (hàng trăm MB hoặc GB) qua một message, bộ nhớ JVM Heap của broker và network buffer sẽ bị nghẽn và sập.
2. **Khả năng chịu lỗi (Fault-Tolerance) & Truyền tải tin cậy:** Nếu truyền nguyên file qua mạng bị đứt ở 99%, toàn bộ file phải truyền lại từ đầu. Khi băm thành các chunk 64KB, hệ thống chỉ cần retry đúng chunk bị lỗi.
3. **Khai thác xử lý phân tán song song (Multi-Partition Concurrency):** Việc chia nhỏ cho phép phân tán các chunk vào nhiều partition (P0, P1, P2) để tận dụng I/O song song.
4. **Bản chất phát video trong bài demo:**
   > *Lưu ý cách nói:* Trong demo này, video được truyền phát dưới dạng các thông điệp Kafka nhị phân độc lập và được gom lại, tái ghép tại phía Consumer trước khi phát (**Client-side Reassembly**). Đây không phải là video streaming dạng progressive phát ngay từ byte đầu tiên, mà là minh hoạ cho kiến trúc chia nhỏ và tái cấu trúc dữ liệu nhị phân qua Kafka.

---

### 3. Vị trí của Kafka trong hệ sinh thái ứng dụng
> **Thầy thường hỏi:** *"Kafka nằm ở đâu? Giữa Frontend - Backend hay giữa Backend - CSDL?"*

- **Không bao giờ đặt Kafka trực tiếp giữa Frontend (Trình duyệt) và Backend:**
  - Trình duyệt chạy trên môi trường người dùng không tin cậy. Mở cổng 9092 của Kafka ra public Internet sẽ đối mặt nguy cơ tấn công DDoS, spam partition làm nghẽn broker.
  - Trình duyệt chỉ giao tiếp qua HTTP/WebSocket, không hỗ trợ giao thức TCP binary thô của Kafka.
  - **Mô hình đúng:** Frontend gửi request/dữ liệu tới Backend API Gateway (Node.js/Java/Go), sau đó Backend sử dụng Kafka Producer API để đẩy vào Broker.
- **Kafka nằm giữa các dịch vụ Backend (Event Streaming Backbone):**
  - Kafka đóng vai trò là xương sống kết nối bất đồng bộ giữa các Microservices độc lập.
  - **Mô hình mở rộng trong thực tế (Future Architecture):**
    ```
    Producer (Upload Service)
               │
               ▼
      [ Kafka Cluster ] (Topic: lecture-stream)
        ├── Consumer Group 1: Video Reassembly & HLS Transcoder Service
        ├── Consumer Group 2: AI Speech-to-Text Service (Whisper)
        └── Consumer Group 3: Archive & S3/Cold-Storage Ingestion Service
    ```
  - **Trong bài demo seminar hiện tại:** Nhóm tập trung chứng minh chính xác các cơ chế nội tại của Kafka thông qua 3 Consumer Groups thực tế:
    1. **`cg-video-streamer`**: Thu thập các chunk nhị phân, ghép lại tệp MP4 hoàn chỉnh và phát video.
    2. **`cg-msg-inspector`**: Soi cấu trúc nội tại của Kafka message (Partition, Offset, Key, Checksum CRC32, Byte range).
    3. **`cg-reassembly-tracker`**: Giám sát bản đồ phân bố chunk trên các Partition và tiến độ reassembly.

---

### 4. Cơ chế Fan-out và Phân bố Partition (Topology chuẩn)
- **Topic:** `stanford-lecture-stream` được chia thành **3 Partitions** (`P0`, `P1`, `P2`).
- **Fan-out:** Cả 3 Consumer Groups đều subscribe cùng topic `stanford-lecture-stream`. Cả 3 đều nhận đủ 100% các chunk được publish.
- **Partition Assignment:** Việc gán partition diễn ra **bên trong từng Consumer Group**. Vì mỗi group ở demo này có 1 consumer worker, worker đó đọc đồng thời cả 3 partition P0, P1, P2 của group mình với offset riêng.
- **Key Routing:** Producer dùng `key = chunk-${chunkIndex}`. Kafka sử dụng thuật toán hash key để phân tán các chunk vào nhiều partition khác nhau (ví dụ: Chunk 0 vào P2, Chunk 1 vào P0, Chunk 2 vào P1...).

---

## 🖥️ PHẦN 2: KỊCH BẢN DEMO TRỰC TIẾP TRÊN LỚP

### Bước 1 — Giới thiệu tổng quan hệ thống
1. Mở song song 2 tab:
   - Dashboard ứng dụng: [http://localhost:3000](http://localhost:3000)
   - Kafka UI quản trị: [http://localhost:8080](http://localhost:8080)
2. Thuyết minh:
   > *"Hệ thống demo của nhóm em sử dụng cụm Apache Kafka KRaft thật chạy trong container Docker, kết nối qua thư viện KafkaJS chuẩn. Phía trên là bảng điều khiển stream và canvas mô phỏng pipeline. Phía dưới là 3 Consumer Groups thực tế đang subscribe vào Topic `stanford-lecture-stream`."*

### Bước 2 — Bấm "▶ Bắt Đầu Stream (Produce)"
1. Quan sát trên giao diện:
   - Video bài giảng Stanford (~770 KB) được Producer băm thành **13 chunks** (mỗi chunk 64 KB).
   - Mỗi chunk là một Kafka message chứa:
     - **Value:** Mảng byte nhị phân thô (`Buffer` raw bytes).
     - **Headers:** Metadata (`chunk-index`, `total-chunks`, `start-byte`, `end-byte`, `checksum`).
     - **Key:** `chunk-${chunkIndex}`.
2. Thuyết minh 3 Consumer Groups nhận dữ liệu độc lập:
   - **Consumer 1 (`cg-video-streamer`):** Nhận từng chunk nhị phân, lưu trữ vào buffer. Khi nhận đủ 13/13 chunks, hệ thống tái ghép thành Blob MP4 và video player phát bài giảng.
   - **Consumer 2 (`cg-msg-inspector`):** Phân tích chi tiết từng message từ Kafka: cho thấy Partition, Offset, Key, mã kiểm tra toàn vẹn CRC32 và preview hex nhị phân.
   - **Consumer 3 (`cg-reassembly-tracker`):** Hiển thị bản đồ phân bố các chunk trên 3 Partitions (P0, P1, P2) và thanh tiến trình tái ghép.

### Bước 3 — Biểu diễn kịch bản "đắt giá": Consumer Lag & Catch-up Replay
1. Tại ô **Consumer 2 (cg-msg-inspector)**, bấm nút **"Ngắt Message Inspector (xem Lag tăng)"**.
   - Thuyết minh: *"Mô phỏng trường hợp Service số 2 bị sự cố sập hoặc mất mạng tạm thời."*
2. Bấm nút **"▶ Bắt Đầu Stream"** lần nữa:
   - Thuyết minh: *"Producer tiếp tục publish đợt dữ liệu mới vào Kafka Broker. Thầy quan sát: Consumer 1 và Consumer 3 vẫn nhận và xử lý bình thường (Lag = 0). Riêng Consumer 2 không tiêu thụ tin nhắn, dẫn tới **TOTAL CONSUMER LAG** tăng vọt (Lag = 13)! Điều này chứng minh tính Decoupling và độc lập offset tuyệt đối giữa các Consumer Groups."*
3. Bấm nút **"▶ Kết nối lại"** tại Consumer 2:
   - Thuyết minh: *"Khi service phục hồi, Kafka kích hoạt cơ chế **Catch-up Replay**, consumer đọc bù tuần tự toàn bộ các message tồn đọng từ committed offset trước đó, và Consumer Lag nhanh chóng trở về 0!"*

### Bước 4 — Chứng minh Kafka thật bằng CLI (Tránh nghi ngờ giả lập)
Mở terminal và gõ trực tiếp lệnh vào container Docker:
```bash
# 1. Kiểm tra Consumer Groups và Lag thật từ broker:
docker exec kafka-kraft-broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --describe --all-groups

# 2. Kiểm tra Offset thật của 3 Partition:
docker exec kafka-kraft-broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:29092 --topic stanford-lecture-stream
```
Thuyết minh: *"Các giá trị offset và lag trên Dashboard của nhóm em phản ánh chính xác 100% với số liệu trả về từ shell script nội bộ của Kafka Broker, không hề sử dụng biến đếm giả lập."*

---

## ❓ PHẦN 3: BỘ CÂU HỎI VẤN ĐÁP CỦA THẦY VÀ CÂU TRẢ LỜI "CHUẨN 10 ĐIỂM"

#### Q1: "Nếu các chunk nằm rải rác trên nhiều partition, làm sao đảm bảo video không bị đảo lộn thứ tự khi ghép lại?"
> **Trả lời tiếng Việt:**  
> *"Dạ thưa Thầy, **Kafka đảm bảo thứ tự tuần tự trong từng partition, chứ không đảm bảo thứ tự toàn cục giữa nhiều partition**. Trong kiến trúc của nhóm em, các chunk được phân tán vào nhiều partition dựa trên key để tối ưu hóa tải. Tuy nhiên, mỗi chunk đều được đóng gói kèm trường `chunk-index` trong metadata header. Phía consumer sẽ sử dụng index này để sắp xếp và tái ghép chính xác dữ liệu gốc trước khi khởi tạo video, đảm bảo không bao giờ bị xáo trộn thứ tự."*
>
> *(Nếu cần trả lời tiếng Anh):*  
> *"Kafka guarantees ordering within a partition, not globally across multiple partitions. In our demo, chunks can be distributed across multiple partitions. The chunk index in the message metadata is used to identify the original position of each chunk, so the consumer can reconstruct the original data order."*

---

#### Q2: "Nếu video dài 2 tiếng nặng 5GB thì hệ thống của em có bị tràn bộ nhớ không?"
> **Trả lời:**  
> *"Dạ thưa Thầy, đối với demo seminar này, nhóm em đọc file video mẫu vào bộ nhớ RAM trước để đơn giản hóa việc minh hoạ.  
> Tuy nhiên, trong một hệ thống Production thực tế, hệ thống sẽ sử dụng cơ chế **Streaming I/O (Read Stream)**:  
> 1. Producer đọc file theo dạng luồng (Node.js `fs.createReadStream` với `highWaterMark = 64KB`), đọc đến đâu băm và publish chunk vào Kafka đến đó mà không bao giờ tải toàn bộ 5GB vào RAM.  
> 2. Phía Kafka Broker được cấu hình **Retention Policy** (theo thời gian như `log.retention.hours=24` hoặc theo dung lượng `log.retention.bytes`) và Consumer sau khi tái ghép sẽ lưu file ra Object Storage (như Amazon S3/MinIO), giúp hệ thống luôn an toàn trước lỗi OOM."*
>
> *(Nếu cần trả lời tiếng Anh):*  
> *"Our seminar demo loads a small sample video into memory for simplicity. In a production system, the producer should use a streaming file/network source and publish chunks incrementally instead of loading the entire file into memory."*

---

#### Q3: "Tại sao trong code lại đặt key là `chunk-${chunkIndex}` thay vì đặt cùng 1 key để vào 1 partition?"
> **Trả lời:**  
> *"Dạ thưa Thầy, việc sử dụng key khác nhau theo từng chunk là chủ ý thiết kế để minh họa khả năng **phân vùng dữ liệu (Partitioning)** của Kafka. Nếu đặt cùng 1 key, Kafka sẽ băm toàn bộ vào duy nhất một partition (ví dụ P0), hai partition P1 và P2 sẽ bị bỏ trống, làm mất đi ý nghĩa của một hệ thống phân tán. Việc chia đều vào P0, P1, P2 giúp Thầy và các bạn thấy rõ cách Kafka phân phối tải trên nhiều partition và cách consumer đọc song song từ nhiều phân vùng."*

---

#### Q4: "Vị trí của Kafka trong source code của nhóm em được tổ chức thế nào?"
> **Trả lời:**  
> *"Dạ thưa Thầy:  
> - Cụm Kafka KRaft chạy độc lập trong Docker container (cổng nội bộ 29092, cổng host 9092).  
> - Trong file `server/kafka-engine.js`, nhóm sử dụng client chính thống `KafkaJS` để khởi tạo `Producer`, `Admin` client và 3 `Consumer` riêng biệt.  
> - File `server/video-chunker.js` chịu trách nhiệm đọc binary và băm thành 64KB buffer.  
> - File `server/server.js` đóng vai trò Backend Gateway điều phối WebSocket đẩy dữ liệu trực quan lên trình duyệt."*

---

#### Q5: "Tại sao không dùng RabbitMQ cho bài toán này mà lại dùng Apache Kafka?"
> **Trả lời:**  
> *"Dạ thưa Thầy, có 3 ưu thế vượt trội của Kafka trong kịch bản này:  
> 1. **Mô hình Fan-out tối ưu:** RabbitMQ phải nhân bản tin nhắn vào nhiều Queue khác nhau trong RAM cho từng bên nhận. Kafka chỉ lưu 1 bản ghi duy nhất trên Commit Log đĩa cứng, và cho phép vô số Consumer Groups cùng đọc với con trỏ offset riêng.  
> 2. **Tính Replayability:** Với RabbitMQ, message bị xóa sau khi ACK. Với Kafka, khi cần tái ghép lại video hoặc khi một consumer mới tham gia hệ thống, consumer có thể reset offset về 0 (`fromBeginning: true`) để đọc lại toàn bộ lịch sử dữ liệu.  
> 3. **Thông lượng & Zero-Copy:** Kafka sử dụng cơ chế ghi đĩa tuần tự (Sequential I/O) và Linux Page Cache kết hợp `sendfile` (Zero-copy), cho phép xử lý hàng triệu gói tin nhị phân mỗi giây với mức tiêu thụ CPU và RAM cực thấp."*
