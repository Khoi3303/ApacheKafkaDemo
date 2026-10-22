const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const kafkaEngine = require('./kafka-engine');

class VideoChunker {
  constructor() {
    this.defaultVideoPath = path.join(
      __dirname,
      'data',
      'sample_video.mp4'
    );

    this.currentVideoBuffer = null;
    this.currentFilename =
      'sample_video.mp4';

    this.chunkSize = 64 * 1024;
    this.streamingInterval = 250;

    this.streamTimer = null;
    this.isStreaming = false;
    this.isPaused = false;
    this.isPublishing = false;

    this.currentChunkIndex = 0;
    this.totalChunks = 0;
    this.streamId = null;

    this.loadDefaultVideo();
  }

  loadDefaultVideo() {
    if (!fs.existsSync(this.defaultVideoPath)) {
      console.warn(
        'Default video not found:',
        this.defaultVideoPath
      );
      return;
    }

    this.currentVideoBuffer =
      fs.readFileSync(this.defaultVideoPath);

    this.calculateTotalChunks();

    console.log(
      `Video loaded: ${this.currentVideoBuffer.length} bytes`
    );

    console.log(
      `Chunk size: ${this.chunkSize} bytes`
    );

    console.log(
      `Total chunks: ${this.totalChunks}`
    );
  }

  setCustomVideo(buffer, filename) {
    this.stopStream();

    this.currentVideoBuffer = buffer;
    this.currentFilename =
      filename || 'custom_upload.mp4';

    this.currentChunkIndex = 0;
    this.streamId =
      `custom-${Date.now().toString(36)}`;

    this.calculateTotalChunks();

    return {
      size: buffer.length,
      filename: this.currentFilename,
      totalChunks: this.totalChunks,
      chunkSize: this.chunkSize,
      streamId: this.streamId
    };
  }

  setChunkSize(newSize) {
    const parsed = parseInt(newSize, 10);

    this.chunkSize =
      parsed > 0
        ? parsed
        : 64 * 1024;

    this.calculateTotalChunks();
  }

  setSpeed(intervalMs) {
    const parsed = parseInt(
      intervalMs,
      10
    );

    this.streamingInterval =
      Math.max(
        20,
        parsed || 250
      );

    if (
      this.isStreaming &&
      !this.isPaused
    ) {
      this.restartTimer();
    }
  }

  calculateTotalChunks() {
    if (!this.currentVideoBuffer) {
      this.totalChunks = 0;
      return;
    }

    this.totalChunks = Math.ceil(
      this.currentVideoBuffer.length /
      this.chunkSize
    );
  }

  async startStream(
    topicName = 'video-stream'
  ) {
    if (!this.currentVideoBuffer) {
      this.loadDefaultVideo();
    }

    if (!this.currentVideoBuffer) {
      throw new Error(
        'No video loaded to stream'
      );
    }

    await kafkaEngine.waitUntilReady();

    if (
      this.isStreaming &&
      this.isPaused
    ) {
      this.isPaused = false;
      this.restartTimer();

      return {
        status: 'resumed'
      };
    }

    this.stopStream();

    this.streamId =
      `video-session-${Date.now().toString(36)}`;

    this.currentChunkIndex = 0;

    this.calculateTotalChunks();

    this.isStreaming = true;
    this.isPaused = false;
    this.isPublishing = false;

    this.topicName = topicName;

    console.log('----------------------------------------------');
    console.log('START STREAM');
    console.log(`Stream ID: ${this.streamId}`);
    console.log(`File: ${this.currentFilename}`);
    console.log(
      `Size: ${this.currentVideoBuffer.length} bytes`
    );
    console.log(
      `Chunk size: ${this.chunkSize} bytes`
    );
    console.log(
      `Total chunks: ${this.totalChunks}`
    );
    console.log('----------------------------------------------');

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

    return {
      status: 'paused',
      currentChunkIndex:
        this.currentChunkIndex
    };
  }

  stopStream() {
    this.isStreaming = false;
    this.isPaused = false;
    this.isPublishing = false;

    if (this.streamTimer) {
      clearInterval(this.streamTimer);
      this.streamTimer = null;
    }

    this.currentChunkIndex = 0;

    return {
      status: 'stopped'
    };
  }

  restartTimer() {
    if (this.streamTimer) {
      clearInterval(this.streamTimer);
    }

    this.streamTimer = setInterval(
      () => {
        this.emitNextChunk().catch(err => {
          console.error(
            'Chunk publishing error:',
            err
          );

          this.stopStream();
        });
      },
      this.streamingInterval
    );
  }

  async emitNextChunk() {
    if (
      !this.isStreaming ||
      this.isPaused ||
      !this.currentVideoBuffer ||
      this.isPublishing
    ) {
      return;
    }

    if (
      this.currentChunkIndex >=
      this.totalChunks
    ) {
      this.finishStream();
      return;
    }

    this.isPublishing = true;

    try {
      const start =
        this.currentChunkIndex *
        this.chunkSize;

      const end = Math.min(
        start + this.chunkSize,
        this.currentVideoBuffer.length
      );

      const chunkBuffer =
        this.currentVideoBuffer.slice(
          start,
          end
        );

      const checksum =
        crypto
          .createHash('md5')
          .update(chunkBuffer)
          .digest('hex')
          .substring(0, 8);

      const chunkIndex =
        this.currentChunkIndex;

      const key =
        `chunk-${chunkIndex}`;

      /*
       * IMPORTANT:
       *
       * Kafka value = RAW BINARY VIDEO CHUNK
       *
       * Kafka headers = metadata
       *
       * This is much closer to a real event-streaming
       * architecture than putting the binary inside JSON.
       */

      const result =
        await kafkaEngine.produce(
          'video-stream',
          key,
          chunkBuffer,
          {
            'content-type': 'video/mp4',
            'stream-id': this.streamId,
            'chunk-index': chunkIndex,
            'total-chunks': this.totalChunks,
            'start-byte': start,
            'end-byte': end,
            'chunk-size': chunkBuffer.length,
            'total-bytes':
              this.currentVideoBuffer.length,
            checksum,
            'source-filename':
              this.currentFilename
          }
        );

      console.log(
        `[PRODUCER] chunk=${chunkIndex} ` +
        `partition=${result.partition} ` +
        `offset=${result.offset} ` +
        `size=${chunkBuffer.length}`
      );

      this.currentChunkIndex++;
    } finally {
      this.isPublishing = false;
    }
  }

  finishStream() {
    const completedStreamId =
      this.streamId;

    const completedChunks =
      this.totalChunks;

    console.log(
      `Stream completed: ${completedStreamId}`
    );

    this.stopStream();

    kafkaEngine.emit(
      'stream:completed',
      {
        streamId: completedStreamId,
        totalChunks: completedChunks
      }
    );
  }

  getStatus() {
    return {
      isStreaming:
        this.isStreaming,

      isPaused:
        this.isPaused,

      currentChunkIndex:
        this.currentChunkIndex,

      totalChunks:
        this.totalChunks,

      chunkSize:
        this.chunkSize,

      streamingInterval:
        this.streamingInterval,

      streamId:
        this.streamId,

      currentFilename:
        this.currentFilename,

      totalBytes:
        this.currentVideoBuffer
          ? this.currentVideoBuffer.length
          : 0,

      progressPercent:
        this.totalChunks > 0
          ? Math.round(
            (this.currentChunkIndex /
              this.totalChunks) *
            100
          )
          : 0
    };
  }
}

module.exports = new VideoChunker();