const express = require('express');
const http = require('http');
const path = require('path');
const cors = require('cors');
const {
  WebSocketServer,
  WebSocket
} = require('ws');

const kafkaEngine =
  require('./kafka-engine');

const videoChunker =
  require('./video-chunker');

const app = express();
const server = http.createServer(app);

const wss =
  new WebSocketServer({
    server,
    path: '/ws'
  });

app.use(cors());

app.use(
  express.json({
    limit: '100mb'
  })
);

app.use(
  express.static(
    path.join(__dirname, '..', 'client')
  )
);

function broadcast(type, data) {
  const payload = JSON.stringify({
    type,
    data,
    timestamp: Date.now()
  });

  for (const client of wss.clients) {
    if (
      client.readyState === WebSocket.OPEN
    ) {
      client.send(payload);
    }
  }
}

/*
 * =====================================================
 * CONSUMER GROUP 1
 * REAL KAFKA CONSUMER
 * =====================================================
 */

kafkaEngine.registerConsumerGroup(
  'cg-video-streamer',
  'video-stream',
  async (msg, meta) => {
    const chunk = msg.value;

    broadcast(
      'consumer:video-stream',
      {
        groupId:
          meta.groupId,

        topic:
          meta.topic,

        partition:
          meta.partition,

        offset:
          meta.offset,

        streamId:
          chunk.streamId,

        chunkIndex:
          chunk.chunkIndex,

        totalChunks:
          chunk.totalChunks,

        chunkSize:
          chunk.chunkSize,

        startByte:
          chunk.startByte,

        endByte:
          chunk.endByte,

        totalBytes:
          chunk.totalBytes,

        mimeType:
          chunk.mimeType,

        base64Data:
          chunk.base64Data,

        checksum:
          chunk.checksum,

        sourceFilename:
          chunk.sourceFilename,

        isCatchUp:
          !!meta.isCatchUp
      }
    );
  }
);

/*
 * =====================================================
 * CONSUMER GROUP 2
 * REAL KAFKA MESSAGE INSPECTOR
 * =====================================================
 */

kafkaEngine.registerConsumerGroup(
  'cg-msg-inspector',
  'video-stream',
  async (msg, meta) => {
    const chunk =
      msg.value;

    const rawBytes =
      msg.rawValue;

    const hexPreview =
      Array.from(
        rawBytes.slice(0, 12)
      )
        .map(byte =>
          byte
            .toString(16)
            .padStart(2, '0')
            .toUpperCase()
        )
        .join(' ');

    broadcast(
      'consumer:msg-inspector',
      {
        groupId:
          meta.groupId,

        topic:
          meta.topic,

        partition:
          meta.partition,

        offset:
          meta.offset,

        chunkIndex:
          chunk.chunkIndex,

        totalChunks:
          chunk.totalChunks,

        sourceFilename:
          chunk.sourceFilename,

        partitionKey:
          msg.key,

        /*
         * IMPORTANT:
         * This partition comes directly from Kafka.
         * We do NOT calculate it ourselves.
         */
        assignedPartition:
          meta.partition,

        startByte:
          chunk.startByte,

        endByte:
          chunk.endByte,

        chunkSize:
          chunk.chunkSize,

        totalBytes:
          chunk.totalBytes,

        checksum:
          chunk.checksum,

        hexPreview,

        isCatchUp:
          !!meta.isCatchUp
      }
    );
  }
);

/*
 * =====================================================
 * CONSUMER GROUP 3
 * REAL KAFKA REASSEMBLY TRACKER
 * =====================================================
 */

const reassemblyState = {
  receivedChunks:
    new Map(),

  partitionOffsets: {
    0: [],
    1: [],
    2: []
  },

  totalBytesAssembled: 0,

  lastFilename:
    null,

  lastStreamId:
    null
};

function resetReassemblyState(
  filename,
  streamId
) {
  reassemblyState.receivedChunks.clear();

  reassemblyState.partitionOffsets = {
    0: [],
    1: [],
    2: []
  };

  reassemblyState.totalBytesAssembled = 0;

  reassemblyState.lastFilename =
    filename || null;

  reassemblyState.lastStreamId =
    streamId || null;
}

kafkaEngine.registerConsumerGroup(
  'cg-reassembly-tracker',
  'video-stream',
  async (msg, meta) => {
    const chunk =
      msg.value;

    if (
      chunk.chunkIndex === 0 ||
      chunk.streamId !==
      reassemblyState.lastStreamId
    ) {
      resetReassemblyState(
        chunk.sourceFilename,
        chunk.streamId
      );
    }

    reassemblyState.receivedChunks.set(
      chunk.chunkIndex,
      {
        partition:
          meta.partition,

        offset:
          meta.offset,

        size:
          chunk.chunkSize
      }
    );

    if (
      !reassemblyState.partitionOffsets[
      meta.partition
      ]
    ) {
      reassemblyState.partitionOffsets[
        meta.partition
      ] = [];
    }

    reassemblyState.partitionOffsets[
      meta.partition
    ].push(meta.offset);

    reassemblyState.totalBytesAssembled +=
      chunk.chunkSize;

    const mapArr =
      Array.from(
        {
          length:
            chunk.totalChunks
        },
        (_, index) =>
          reassemblyState.receivedChunks.has(
            index
          )
            ? '■'
            : '□'
      );

    const partitionDistribution =
      [0, 1, 2].map(partition => {
        const offsets =
          reassemblyState
            .partitionOffsets[partition] ||
          [];

        return (
          `P${partition}: ` +
          `${offsets.length} chunks`
        );
      });

    broadcast(
      'consumer:reassembly-tracker',
      {
        groupId:
          meta.groupId,

        topic:
          meta.topic,

        partition:
          meta.partition,

        offset:
          meta.offset,

        chunkIndex:
          chunk.chunkIndex,

        totalChunks:
          chunk.totalChunks,

        sourceFilename:
          chunk.sourceFilename,

        startByte:
          chunk.startByte,

        endByte:
          chunk.endByte,

        chunkSize:
          chunk.chunkSize,

        checksum:
          chunk.checksum,

        totalBytesAssembled:
          reassemblyState.totalBytesAssembled,

        totalBytes:
          chunk.totalBytes,

        receivedCount:
          reassemblyState.receivedChunks.size,

        chunkMap:
          mapArr.join(''),

        partitionDistribution,

        isCatchUp:
          !!meta.isCatchUp
      }
    );
  }
);

/*
 * =====================================================
 * KAFKA EVENTS -> FRONTEND
 * =====================================================
 */

kafkaEngine.on(
  'message:produced',
  data => {
    broadcast(
      'kafka:produced',
      data
    );
  }
);

kafkaEngine.on(
  'stream:completed',
  data => {
    broadcast(
      'stream:completed',
      data
    );
  }
);

kafkaEngine.on(
  'consumer:paused',
  data => {
    broadcast(
      'consumer:paused',
      data
    );
  }
);

kafkaEngine.on(
  'consumer:resumed',
  data => {
    broadcast(
      'consumer:resumed',
      data
    );
  }
);

/*
 * =====================================================
 * TELEMETRY
 * =====================================================
 */

setInterval(async () => {
  if (wss.clients.size === 0) {
    return;
  }

  try {
    await kafkaEngine.refreshSnapshot();

    broadcast(
      'telemetry:snapshot',
      {
        kafka:
          kafkaEngine.getSnapshot(),

        chunker:
          videoChunker.getStatus()
      }
    );
  } catch (err) {
    console.error(
      'Telemetry error:',
      err.message
    );
  }
}, 500);

/*
 * =====================================================
 * REST API
 * =====================================================
 */

app.get(
  '/api/snapshot',
  async (req, res) => {
    try {
      await kafkaEngine.refreshSnapshot();

      res.json({
        kafka:
          kafkaEngine.getSnapshot(),

        chunker:
          videoChunker.getStatus()
      });
    } catch (err) {
      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

app.post(
  '/api/stream/start',
  async (req, res) => {
    try {
      const result =
        await videoChunker.startStream();

      res.json({
        success: true,
        result
      });
    } catch (err) {
      res.status(400).json({
        success: false,
        error: err.message
      });
    }
  }
);

app.post(
  '/api/stream/pause',
  (req, res) => {
    const result =
      videoChunker.pauseStream();

    res.json({
      success: true,
      result
    });
  }
);

app.post(
  '/api/stream/stop',
  (req, res) => {
    const result =
      videoChunker.stopStream();

    res.json({
      success: true,
      result
    });
  }
);

app.post(
  '/api/stream/config',
  (req, res) => {
    const {
      chunkSize,
      speedInterval
    } = req.body;

    if (chunkSize) {
      videoChunker.setChunkSize(
        chunkSize
      );
    }

    if (speedInterval) {
      videoChunker.setSpeed(
        speedInterval
      );
    }

    res.json({
      success: true,
      status:
        videoChunker.getStatus()
    });
  }
);

app.post(
  '/api/consumer/:groupId/pause',
  async (req, res) => {
    try {
      const result =
        await kafkaEngine.pauseConsumerGroup(
          req.params.groupId
        );

      res.json({
        success: result,
        groupId:
          req.params.groupId,
        paused: true
      });
    } catch (err) {
      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

app.post(
  '/api/consumer/:groupId/resume',
  async (req, res) => {
    try {
      const result =
        await kafkaEngine.resumeConsumerGroup(
          req.params.groupId
        );

      res.json({
        success: result,
        groupId:
          req.params.groupId,
        paused: false
      });
    } catch (err) {
      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

app.post(
  '/api/system/reset',
  async (req, res) => {
    try {
      videoChunker.stopStream();

      resetReassemblyState(
        videoChunker.currentFilename,
        null
      );

      await kafkaEngine.resetAll();

      broadcast(
        'system:reset',
        {}
      );

      res.json({
        success: true
      });
    } catch (err) {
      console.error(
        'Reset error:',
        err
      );

      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

/*
 * =====================================================
 * VIDEO UPLOAD
 * =====================================================
 */

app.post(
  '/api/stream/upload',
  (req, res) => {
    const {
      filename,
      base64Content
    } = req.body;

    if (!base64Content) {
      return res.status(400).json({
        success: false,
        error:
          'No content provided'
      });
    }

    try {
      const buffer =
        Buffer.from(
          base64Content,
          'base64'
        );

      const result =
        videoChunker.setCustomVideo(
          buffer,
          filename
        );

      resetReassemblyState(
        filename,
        result.streamId
      );

      res.json({
        success: true,
        result
      });
    } catch (err) {
      res.status(500).json({
        success: false,
        error: err.message
      });
    }
  }
);

/*
 * =====================================================
 * WEBSOCKET
 * =====================================================
 */

wss.on(
  'connection',
  async ws => {
    try {
      await kafkaEngine.waitUntilReady();

      await kafkaEngine.refreshSnapshot();

      ws.send(
        JSON.stringify({
          type: 'welcome',
          data: {
            message:
              'Connected to REAL Apache Kafka',
            snapshot:
              kafkaEngine.getSnapshot(),
            chunker:
              videoChunker.getStatus()
          }
        })
      );
    } catch (err) {
      ws.send(
        JSON.stringify({
          type: 'error',
          data: {
            message:
              'Kafka is not ready',
            error:
              err.message
          }
        })
      );
    }
  }
);

const PORT =
  process.env.PORT || 3000;

server.listen(
  PORT,
  () => {
    console.log(
      '===================================================='
    );

    console.log(
      ' Apache Kafka Video Streaming Demo'
    );

    console.log(
      ` Server: http://localhost:${PORT}`
    );

    console.log(
      ' Kafka: REAL Apache Kafka'
    );

    console.log(
      ' Topic: video-stream'
    );

    console.log(
      ' Consumer Groups: 3'
    );

    console.log(
      '===================================================='
    );
  }
);