# 🎤 KỊCH BẢN THUYẾT TRÌNH BẢO VỆ ĐỀ TÀI APACHE KAFKA (20%)
> **Dành cho nhóm sinh viên:** Bám sát tuyệt đối hướng dẫn chấm điểm của Thầy (WHAT, WHY, HOW, WHEN) và trả lời xuất sắc câu hỏi hóc búa của Thầy.

---

## ⏱️ PHÂN BỔ THỜI GIAN THUYẾT TRÌNH (TỔNG: 10 - 15 PHÚT)
- **Phần 1: Khía cạnh lý thuyết (3 - 4 phút):** WHAT, WHY, HOW, WHEN & Vị trí Kafka trong kiến trúc.
- **Phần 2: Demo thực hành trực quan (5 - 7 phút):** Thao tác trên Web Dashboard, chứng minh Băm nhỏ dữ liệu và 1 Nguồn phát - Nhiều Nguồn thu.
- **Phần 3: Vấn đáp & Trả lời câu hỏi của Thầy (3 - 5 phút):** Bộ câu hỏi - câu trả lời "tủ".

---

## 📑 PHẦN 1: NỘI DUNG LÝ THUYẾT (SLIDES / NÓI)

### 1. WHAT (Đề tài trình bày nội dung gì, công cụ gì?)
> *"Kính thưa Thầy và các bạn, nhóm chúng em xin trình bày về **Apache Kafka** - Nền tảng truyền phát sự kiện phân tán (Distributed Event Streaming Platform) mạnh mẽ nhất hiện nay.*  
> *Ba đặc trưng kỹ thuật cốt lõi của Kafka:*
> 1. *Khả năng xuất bản và đăng ký nhận thông điệp (Publish - Subscribe) với thông lượng hàng triệu message/giây.*
> 2. *Khả năng lưu trữ phân tán bền vững trên đĩa cứng dưới dạng **Append-only Commit Log** có khả năng nhân bản và chịu lỗi.*
> 3. *Khả năng xử lý dòng dữ liệu theo thời gian thực (Real-time Stream Processing)."*

---

### 2. WHY (Vì sao cần khai thác? Vì sao phải băm nhỏ dữ liệu?)
> *"Thầy đã đặt ra câu hỏi rất quan trọng: **Tại sao khi truyền video 1GB ta không truyền cả file qua đường truyền mà phải băm nhỏ nó ra?** Nhóm em xin làm rõ 4 lý do sống còn:*
> 1. ***Độ trễ thời gian thực (Zero Latency Streaming):** Nếu truyền nguyên file 1GB qua HTTP hoặc lưu vào SQL BLOB, người xem phải đợi tải xong 100% (hàng chục phút) mới xem được. Khi băm thành các chunk 64KB, người dùng có thể xem ngay giây đầu tiên khi Chunk 0 vừa đến trong khi Server vẫn đang tiếp tục truyền Chunk 100.*
> 2. ***Bảo vệ bộ nhớ (Chống OOM Crash):** Kafka được thiết kế tối ưu cho message từ vài KB đến 1MB. Truyền 1GB sẽ làm nổ tung bộ nhớ JVM Heap của cả Broker và Consumer.*
> 3. ***Độ chịu lỗi (Fault-Tolerance):** Nếu đường truyền bị ngắt khi tải 99% của file 1GB, toàn bộ 1GB phải tải lại từ đầu. Với Kafka, hệ thống chỉ cần retry lại đúng chunk 64KB bị lỗi.*
> 4. ***Xử lý song song đa nhiệm (Parallel Pipelines):** Mô hình AI có thể vừa dịch phụ đề cho Chunk 1 trong khi Producer đang truyền Chunk 5."*

---

### 3. VỊ TRÍ KAFKA TRONG SOURCE CODE: "Giữa Frontend - Backend hay giữa Backend - SQL?"
> *(Đây là câu hỏi trọng tâm của Thầy, nhóm cần khẳng định tự tin theo 3 luận điểm sau):*
>
> 🔹 **1. Có nên đặt Kafka giữa Frontend và Backend không?**  
> ➔ **TRẢ LỜI: KHÔNG NÊN trong môi trường Production.**  
> - *Lý do:* Trình duyệt web chạy trên môi trường không an toàn. Nếu mở cổng 9092 của Kafka ra public Internet, kẻ xấu có thể tấn công DDoS, spam partition làm sập cụm broker. Ngoài ra, trình duyệt chỉ hỗ trợ HTTP/WebSocket, không hỗ trợ TCP socket thô của Kafka.  
> - *Cách làm đúng chuẩn:* Frontend gửi dữ liệu qua HTTP REST hoặc WebSocket tới **Backend API Gateway / Ingestion Service**, sau đó Backend dùng **Kafka Producer API** đẩy vào Kafka.
>
> 🔹 **2. Có nên đặt Kafka giữa Backend và SQL không?**  
> ➔ **TRẢ LỜI: RẤT NÊN VÀ RẤT PHỔ BIẾN!**  
> - *Lý do:* Đây là mô hình **Async Write Buffering (Chống sập Database)**. Khi hàng trăm nghìn video chunk hoặc lượt click dồn về mỗi giây, nếu ghi thẳng vào SQL Database, database sẽ bị nghẽn connection và treo cứng. Kafka đứng giữa làm vùng đệm chịu tải. Consumer sẽ đọc dữ liệu từ Kafka và ghi vào SQL theo từng đợt (**Batch Insert**) với tốc độ an toàn.  
> - *Mô hình ngược lại (CDC):* Sử dụng Debezium để bắt thay đổi trong log của SQL rồi đẩy vào Kafka để đồng bộ sang các hệ thống khác.
>
> 🔹 **3. Kết luận vị trí tối ưu nhất của Kafka:**  
> ➔ **Kafka là "Xương sống sự kiện" (Distributed Event Backbone) nằm giữa các Microservices độc lập ở tầng Backend.**

---

### 4. HOW & WHEN (Cách hoạt động và Hoàn cảnh sử dụng)
> 🔹 **HOW: Cơ chế 1 Nguồn phát nhưng Nhiều Nguồn thu (Pub/Sub Broadcast):**  
> - *"Trong Queue truyền thống (RabbitMQ), tin nhắn khi được đọc sẽ bị xóa khỏi hàng đợi.*  
> - *Trong Kafka, tin nhắn được ghi nối tiếp vào các **Partition Log** và lưu trữ cố định. Mỗi bên thu đăng ký một **Consumer Group** riêng biệt và tự quản lý con trỏ **Offset** của mình.*  
> - *Nhờ vậy, Kafka tự động **Broadcast (Fan-out)** 1 nguồn phát video tới cả 3 nguồn thu: 1 nguồn phát video, 1 nguồn dịch phụ đề AI, và 1 nguồn ghi CSDL mà không bên nào cản trở bên nào!"*
>
> 🔹 **WHEN (Khi nào dùng & Khi nào không dùng):**  
> - *NÊN DÙNG:* Khi cần xử lý lưu lượng lớn liên tục (video chunks, IoT, clickstream), kiến trúc microservices bất đồng bộ, hệ thống cần tính phát lại (Replayability).  
> - *KHÔNG NÊN DÙNG:* Ứng dụng CRUD đơn giản, lưu lượng thấp, hoặc các tương tác đòi hỏi đồng bộ tức thời (Synchronous RPC).

---

## 🖥️ PHẦN 2: KỊCH BẢN THỰC HÀNH DEMO TRỰC TIẾP TRÊN LỚP

1. **Khởi động:** Mở trình duyệt tại [http://localhost:3000](http://localhost:3000).
2. **Chỉ cho Thầy thấy giao diện:**
   - *"Thưa Thầy, giao diện của nhóm em gồm: Thanh điều khiển kích thước Chunk (32KB - 256KB), Tốc độ stream, Pipeline đồ họa Canvas thời gian thực, 3 Consumer Groups và Bảng nhật ký Partition Offsets."*
3. **Bấm "▶ Bắt Đầu Stream (Produce)":**
   - *"Thầy có thể thấy video bài giảng Stanford dung lượng ~770KB được Producer băm thành 13 chunks (mỗi chunk 64KB).*
   - *Trên đồ họa Canvas, các gói tin được đẩy vào 3 Partition (P0, P1, P2) của Topic `stanford-lecture-stream`.*
   - *Tại điểm giao thoa, Kafka thực hiện cơ chế **Fan-out Broadcast** đồng thời đến cả 3 Consumer Groups:*
     - *Consumer 1: Nhận từng chunk nhị phân và tái ghép mảng bytes, video bài giảng bắt đầu phát mượt mà.*
     - *Consumer 2: Nhận cùng lúc các chunk để AI Whisper-v3 hiển thị phụ đề bài giảng Stanford theo thời gian thực.*
     - *Consumer 3: Ghi nhận ngay các bản ghi vào CSDL SQL."*
4. **Hành động đắt giá nhất để ghi điểm cao:**
   - Bấm nút **"⏸ Tạm dừng Nhận"** tại **Consumer 2 (AI Speech-to-Text)**.
   - Thuyết minh: *"Thầy quan sát: Nhóm AI vừa bị tạm dừng, chỉ số **Consumer Lag** của nhóm 2 lập tức tăng lên (Lag = 3, 4, 5...). TUY NHIÊN, Consumer 1 (Video Player) và Consumer 3 (SQL) vẫn tiếp tục nhận và chạy mượt mà không hề bị ảnh hưởng! Đây chính là minh chứng sống động cho tính **Decoupling** và độc lập Offset của Kafka!"*
   - Bấm nút **"▶ Tiếp tục Nhận"** tại Consumer 2.
   - Thuyết minh: *"Khi nhóm AI hoạt động trở lại, Kafka kích hoạt cơ chế **Catch-up / Offset Replay**, nhóm 2 đọc bù toàn bộ các offset bị lỡ và Lag quay trở về 0!"*
5. **Chuyển sang Tab "Kafka Nằm Ở Đâu?":**
   - Chỉ vào sơ đồ kiến trúc và mẫu code Node.js đã chuẩn bị sẵn để giải đáp trọn vẹn cho câu hỏi của Thầy.

---

## ❓ PHẦN 3: BỘ CÂU HỎI VẤN ĐÁP CỦA THẦY VÀ CÂU TRẢ LỜI "CHUẨN 10 ĐIỂM"

#### Q1: "Nếu 1 video dài 2 tiếng nặng 5GB thì Kafka có lưu toàn bộ 5GB đó trong topic không?"
> **Trả lời:**  
> *"Dạ thưa Thầy, có thể lưu nhưng thực tế người ta sẽ cấu hình **Retention Policy** (theo thời gian, ví dụ 1 ngày, hoặc theo dung lượng, ví dụ 10GB). Các chunk sau khi được xử lý và ghi xuống Cloud Storage (như Amazon S3 hay MinIO) bởi Consumer Group 3 thì Kafka có thể tự động xóa dọn log cũ theo cấu hình `log.retention.hours` hoặc `log.cleanup.policy=delete`, giúp cụm broker luôn nhẹ nhàng và tối ưu chi phí."*

#### Q2: "Làm sao đảm bảo các chunk của video không bị đảo lộn thứ tự khi phát?"
> **Trả lời:**  
> *"Dạ thưa Thầy, Kafka đảm bảo **thứ tự tuần tự tuyệt đối (Strict Ordering) trong phạm vi TỪNG PARTITION**. Khi băm nhỏ, Producer sử dụng chung một `message.key = streamId`, Kafka sẽ dùng thuật toán băm khóa (Hash Key) để định tuyến toàn bộ các chunk của cùng một video vào ĐÚNG MỘT PARTITION duy nhất (ví dụ Partition 0). Nhờ vậy, offset của các chunk sẽ luôn tăng tuần tự 0, 1, 2, 3... và Consumer chắc chắn nhận đúng thứ tự mà không sợ bị xáo trộn."*

#### Q3: "Tại sao không dùng RabbitMQ cho bài toán này mà lại dùng Kafka?"
> **Trả lời:**  
> *"Dạ thưa Thầy, có 3 điểm khác biệt lớn:*  
> *1. **Thông lượng:** Kafka xử lý bằng cơ chế ghi đĩa tuần tự và Zero-copy của Kernel nên đạt hàng triệu msg/s, vượt trội hơn RabbitMQ vốn phụ thuộc nhiều vào RAM broker.*  
> *2. **Mô hình Broadcast:** RabbitMQ dùng cơ chế xóa tin nhắn sau khi consumer đọc xong; nếu muốn broadcast tới nhiều ứng dụng phải tạo thêm nhiều exchange và queue riêng biệt, gây nhân bản dữ liệu trong RAM. Kafka dùng mô hình Log phân vùng, cùng 1 tin nhắn trên đĩa cứng có thể phục vụ vô số Consumer Groups độc lập.*  
> *3. **Replayability:** Khi mô hình AI muốn chạy lại từ đầu dữ liệu video, Kafka chỉ cần reset offset về 0; còn RabbitMQ thì tin nhắn đã bị xóa mất."*
