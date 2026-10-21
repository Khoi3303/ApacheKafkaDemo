/**
 * Video Chunker & Producer Service
 * 
 * Implements chunking of large media files (Stanford Lecture Video) into small Kafka events
 * instead of sending monolithic gigabytes over the wire.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const kafkaEngine = require('./kafka-engine');

// Generic Kafka-themed transcript segments — adapted at runtime per file
const GENERIC_TRANSCRIPT_SEGMENTS = [
  "[Kafka Producer] Bắt đầu chia nhỏ tệp thành các chunk binary và ghi vào topic.",
  "[Chunk #1] Header container nhận được – phát hiện codec và metadata video.",
  "[Chunk #2] Dữ liệu audio/video được đóng gói dưới dạng KafkaMessage với offset tuần tự.",
  "[Kafka Broker] Tin nhắn được ghi vào Partition Log (append-only, bất biến).",
  "[Consumer 1] Reassembling binary blob – tái ghép các chunk thành luồng video liên tục.",
  "[Consumer 2] AI đang phân tích audio stream – trích xuất features từ chunk hiện tại.",
  "[Chunk offset] Mỗi Consumer Group duy trì con trỏ Offset riêng, đọc hoàn toàn độc lập.",
  "[Kafka Design] 1 Producer phát tin, 3 Consumer Groups nhận đồng thời – Fan-out Broadcast.",
  "[Consumer Lag] Nếu Consumer chậm xử lý, lag tăng – Kafka vẫn giữ dữ liệu trong log.",
  "[Partitioning] Chunk được hash theo partition key và phân phối đều trên 3 Partitions.",
  "[Consumer 3] Ghi nhật ký metadata chunk vào bảng SQL: chunk_index, offset, checksum.",
  "[Zero-Copy] Kafka sử dụng sendfile() để chuyển dữ liệu không qua userspace – cực hiệu quả.",
  "[Offset Replay] Khi Consumer kết nối lại, nó đọc bù từ committed offset cuối cùng.",
  "[High Throughput] Throughput đạt được nhờ batch processing và sequential disk writes.",
  "[Real World] Netflix, LinkedIn, Uber dùng Kafka xử lý hàng tỷ sự kiện mỗi ngày.",
  "[Fan-out] Cùng 1 message được 3 Consumer Groups xử lý theo cách hoàn toàn khác nhau.",
  "[Stream Complete] Tất cả chunks đã được publish – Consumer Groups tiếp tục drain lag.",
  "[Summary] Kafka: append-only log + độc lập Consumer + retain dữ liệu = Event Streaming.",
  "[Demo End] Resetting offset counters. Hệ thống sẵn sàng stream lại."
];

class VideoChunker {
  constructor() {
    this.defaultVideoPath = path.join(__dirname, 'data', 'stanford_lecture_sample.mp4');
    this.currentVideoBuffer = null;
    this.currentFilename = 'stanford_lecture_sample.mp4';
    this.chunkSize = 64 * 1024; // 64 KB default chunk size
    this.streamingInterval = 250; // ms between chunks (4 chunks/sec)
    this.streamTimer = null;
    this.isStreaming = false;
    this.isPaused = false;
    this.currentChunkIndex = 0;
    this.totalChunks = 0;
    this.streamId = null;

    this.loadDefaultVideo();
  }

  loadDefaultVideo() {
    try {
      if (fs.existsSync(this.defaultVideoPath)) {
        this.currentVideoBuffer = fs.readFileSync(this.defaultVideoPath);
        this.calculateTotalChunks();
        console.log(`Loaded default Stanford video: ${this.currentVideoBuffer.length} bytes, ${this.totalChunks} chunks (@ ${this.chunkSize / 1024} KB).`);
      } else {
        console.warn('Default video file not found at:', this.defaultVideoPath);
      }
    } catch (err) {
      console.error('Error loading default video:', err);
    }
  }

  setCustomVideo(buffer, filename) {
    this.stopStream();
    this.currentVideoBuffer = buffer;
    this.currentFilename = filename || 'custom_upload.mp4';
    this.currentChunkIndex = 0;
    this.streamId = `custom-${Date.now().toString(36)}`;
    this.calculateTotalChunks();
    console.log(`Loaded custom video "${this.currentFilename}": ${buffer.length} bytes, ${this.totalChunks} chunks.`);
    return {
      size: buffer.length,
      filename: this.currentFilename,
      totalChunks: this.totalChunks,
      chunkSize: this.chunkSize,
      streamId: this.streamId
    };
  }

  setChunkSize(newSize) {
    this.chunkSize = parseInt(newSize, 10) || (64 * 1024);
    this.calculateTotalChunks();
  }

  setSpeed(intervalMs) {
    this.streamingInterval = Math.max(20, parseInt(intervalMs, 10) || 250);
    if (this.isStreaming && !this.isPaused) {
      this.restartTimer();
    }
  }

  calculateTotalChunks() {
    if (!this.currentVideoBuffer) {
      this.totalChunks = 0;
      return;
    }
    this.totalChunks = Math.ceil(this.currentVideoBuffer.length / this.chunkSize);
  }

  startStream(topicName = 'stanford-lecture-stream', partitionCount = 3) {
    if (!this.currentVideoBuffer) {
      this.loadDefaultVideo();
      if (!this.currentVideoBuffer) {
        throw new Error('No video loaded to stream');
      }
    }

    if (this.isStreaming && this.isPaused) {
      this.isPaused = false;
      this.restartTimer();
      return { status: 'resumed' };
    }

    this.stopStream();
    this.streamId = `stanford-cs229-${Date.now().toString(36)}`;
    this.currentChunkIndex = 0;
    this.calculateTotalChunks();
    this.isStreaming = true;
    this.isPaused = false;

    // Ensure topic exists in Kafka
    kafkaEngine.createTopic(topicName, partitionCount);

    this.restartTimer();

    return {
      status: 'started',
      streamId: this.streamId,
      totalChunks: this.totalChunks,
      chunkSize: this.chunkSize,
      totalBytes: this.currentVideoBuffer.length
    };
  }

  pauseStream() {
    this.isPaused = true;
    if (this.streamTimer) {
      clearInterval(this.streamTimer);
      this.streamTimer = null;
    }
    return { status: 'paused', currentChunkIndex: this.currentChunkIndex };
  }

  stopStream() {
    this.isStreaming = false;
    this.isPaused = false;
    if (this.streamTimer) {
      clearInterval(this.streamTimer);
      this.streamTimer = null;
    }
    this.currentChunkIndex = 0;
    return { status: 'stopped' };
  }

  restartTimer() {
    if (this.streamTimer) {
      clearInterval(this.streamTimer);
    }
    this.streamTimer = setInterval(() => {
      this.emitNextChunk();
    }, this.streamingInterval);
  }

  emitNextChunk() {
    if (!this.isStreaming || this.isPaused || !this.currentVideoBuffer) return;

    if (this.currentChunkIndex >= this.totalChunks) {
      console.log('Video stream completed! All chunks published to Kafka.');
      this.stopStream();
      kafkaEngine.emit('stream:completed', { streamId: this.streamId, totalChunks: this.totalChunks });
      return;
    }

    const start = this.currentChunkIndex * this.chunkSize;
    const end = Math.min(start + this.chunkSize, this.currentVideoBuffer.length);
    const chunkSlice = this.currentVideoBuffer.slice(start, end);

    // Compute checksum
    const hash = crypto.createHash('md5').update(chunkSlice).digest('hex').substring(0, 8);

    // Pick transcript segment dynamically — adapts to any file name & progress
    const transcriptIdx = Math.floor((this.currentChunkIndex / this.totalChunks) * GENERIC_TRANSCRIPT_SEGMENTS.length);
    const transcriptText = GENERIC_TRANSCRIPT_SEGMENTS[Math.min(transcriptIdx, GENERIC_TRANSCRIPT_SEGMENTS.length - 1)];

    // Standard Kafka Event Message Schema
    const chunkEvent = {
      streamId: this.streamId,
      chunkIndex: this.currentChunkIndex,
      totalChunks: this.totalChunks,
      startByte: start,
      endByte: end,
      chunkSize: chunkSlice.length,
      totalBytes: this.currentVideoBuffer.length,
      mimeType: 'video/mp4',
      checksum: hash,
      timestamp: Date.now(),
      base64Data: chunkSlice.toString('base64'),
      transcriptSegment: transcriptText,
      sourceFilename: this.currentFilename,
      metadata: {
        filename: this.currentFilename,
        topic: "Apache Kafka Event-Driven Streaming",
        segmentSeconds: (this.currentChunkIndex * (this.streamingInterval / 1000)).toFixed(1),
        progress: `${this.currentChunkIndex + 1}/${this.totalChunks}`
      }
    };

    // Partition key: streamId guarantees order if needed, or null for round-robin
    const partitionKey = `chunk-${this.currentChunkIndex}`;
    
    // Produce message to Kafka Broker
    kafkaEngine.produce(
      'stanford-lecture-stream',
      partitionKey,
      chunkEvent,
      {
        producer: 'stanford-lecture-ingest-service',
        schema: 'video-chunk-v1'
      }
    );

    this.currentChunkIndex++;
  }

  getStatus() {
    return {
      isStreaming: this.isStreaming,
      isPaused: this.isPaused,
      currentChunkIndex: this.currentChunkIndex,
      totalChunks: this.totalChunks,
      chunkSize: this.chunkSize,
      streamingInterval: this.streamingInterval,
      streamId: this.streamId,
      currentFilename: this.currentFilename,
      totalBytes: this.currentVideoBuffer ? this.currentVideoBuffer.length : 0,
      progressPercent: this.totalChunks > 0 ? Math.round((this.currentChunkIndex / this.totalChunks) * 100) : 0
    };
  }
}

module.exports = new VideoChunker();
