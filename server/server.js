/**
 * Apache Kafka Demo Server
 * Express + WebSocket + Kafka Streaming Engine
 */

const express = require('express');
const http = require('http');
const path = require('path');
const cors = require('cors');
const { WebSocketServer, WebSocket } = require('ws');
const kafkaEngine = require('./kafka-engine');
const videoChunker = require('./video-chunker');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

app.use(cors());
app.use(express.json({ limit: '100mb' }));
app.use(express.static(path.join(__dirname, '..', 'client')));



// WebSocket broadcast helper
function broadcast(type, data) {
  const payload = JSON.stringify({ type, data, timestamp: Date.now() });
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

// ----------------------------------------------------
// REGISTER KAFKA CONSUMER GROUPS (Broadcast / Pub-Sub)
// ----------------------------------------------------

// CONSUMER GROUP 1: Live Video Player Streamer
kafkaEngine.registerConsumerGroup('cg-video-streamer', 'stanford-lecture-stream', (msg, meta) => {
  const chunk = msg.value;
  broadcast('consumer:video-stream', {
    groupId: meta.groupId,
    partition: meta.partition,
    offset: meta.offset,
    streamId: chunk.streamId,
    chunkIndex: chunk.chunkIndex,
    totalChunks: chunk.totalChunks,
    chunkSize: chunk.chunkSize,
    mimeType: chunk.mimeType || 'video/mp4',
    base64Data: chunk.base64Data, // Video bytes to play in browser
    checksum: chunk.checksum,
    isCatchUp: !!meta.isCatchUp
  });
});

// CONSUMER GROUP 2: Kafka Message Inspector
// Shows REAL Kafka internals: partition routing (key hash), offset, byte range, hex preview
kafkaEngine.registerConsumerGroup('cg-msg-inspector', 'stanford-lecture-stream', (msg, meta) => {
  const chunk = msg.value;

  // Compute the same hash the producer used for partition routing
  const key = `chunk-${chunk.chunkIndex}`;
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash = (hash << 5) - hash + key.charCodeAt(i);
    hash |= 0;
  }
  const partitionFromHash = Math.abs(hash) % 3;

  // Decode first 8 bytes of the chunk as hex (from base64)
  const rawBytes = Buffer.from(chunk.base64Data, 'base64');
  const hexPreview = Array.from(rawBytes.slice(0, 12))
    .map(b => b.toString(16).padStart(2, '0').toUpperCase())
    .join(' ');

  broadcast('consumer:msg-inspector', {
    groupId: meta.groupId,
    partition: meta.partition,
    offset: meta.offset,
    chunkIndex: chunk.chunkIndex,
    totalChunks: chunk.totalChunks,
    sourceFilename: chunk.sourceFilename,
    // Routing detail
    partitionKey: key,
    keyHashHex: `0x${(Math.abs(hash) >>> 0).toString(16).toUpperCase().padStart(8, '0')}`,
    keyHashMod: `${Math.abs(hash)} % 3 = ${partitionFromHash}`,
    assignedPartition: meta.partition,
    // Byte detail
    startByte: chunk.startByte,
    endByte: chunk.endByte,
    chunkSize: chunk.chunkSize,
    totalBytes: chunk.totalBytes,
    checksum: chunk.checksum,
    hexPreview,
    isCatchUp: !!meta.isCatchUp
  });
});


// CONSUMER GROUP 3: Chunk Reassembly Tracker
// Tracks how chunks from different partitions accumulate and reassemble
const reassemblyState = {
  receivedChunks: new Map(), // chunkIndex -> { partition, offset, size }
  partitionOffsets: { 0: [], 1: [], 2: [] }, // per-partition offset sequences
  totalBytesAssembled: 0,
  lastFilename: null,
  lastStreamId: null
};

function resetReassemblyState(filename, streamId) {
  reassemblyState.receivedChunks.clear();
  reassemblyState.partitionOffsets = { 0: [], 1: [], 2: [] };
  reassemblyState.totalBytesAssembled = 0;
  reassemblyState.lastFilename = filename || null;
  reassemblyState.lastStreamId = streamId || null;
}

kafkaEngine.registerConsumerGroup('cg-reassembly-tracker', 'stanford-lecture-stream', (msg, meta) => {
  const chunk = msg.value;

  // Reset tracker when new stream starts (chunkIndex 0, streamId changes, or filename changes)
  if (chunk.chunkIndex === 0 || chunk.streamId !== reassemblyState.lastStreamId || chunk.sourceFilename !== reassemblyState.lastFilename) {
    resetReassemblyState(chunk.sourceFilename, chunk.streamId);
  }

  // Record this chunk
  reassemblyState.receivedChunks.set(chunk.chunkIndex, {
    partition: meta.partition,
    offset: meta.offset,
    size: chunk.chunkSize
  });
  reassemblyState.partitionOffsets[meta.partition].push(meta.offset);
  reassemblyState.totalBytesAssembled += chunk.chunkSize;

  // Build chunk map string: each position is ■ (received) or □ (missing)
  const mapArr = Array.from({ length: chunk.totalChunks }, (_, i) =>
    reassemblyState.receivedChunks.has(i) ? '■' : '□'
  );

  // Distribution per partition
  const partitionDist = [0, 1, 2].map(p => {
    const offsets = reassemblyState.partitionOffsets[p];
    return `P${p}: ${offsets.length} chunks [${offsets.slice(-3).join(',')}${offsets.length > 3 ? '...' : ''}]`;
  });

  broadcast('consumer:reassembly-tracker', {
    groupId: meta.groupId,
    partition: meta.partition,
    offset: meta.offset,
    chunkIndex: chunk.chunkIndex,
    totalChunks: chunk.totalChunks,
    sourceFilename: chunk.sourceFilename,
    startByte: chunk.startByte,
    endByte: chunk.endByte,
    chunkSize: chunk.chunkSize,
    checksum: chunk.checksum,
    totalBytesAssembled: reassemblyState.totalBytesAssembled,
    totalBytes: chunk.totalBytes,
    receivedCount: reassemblyState.receivedChunks.size,
    chunkMap: mapArr.join(''),
    partitionDistribution: partitionDist,
    isCatchUp: !!meta.isCatchUp
  });
});


// Forward Kafka internal events to WebSockets
kafkaEngine.on('message:produced', (data) => {
  broadcast('kafka:produced', data);
});

kafkaEngine.on('consumer:lagged', (data) => {
  broadcast('kafka:lagged', data);
});

kafkaEngine.on('stream:completed', (data) => {
  broadcast('stream:completed', data);
});

// Send periodic cluster status snapshot every 500ms
setInterval(() => {
  if (wss.clients.size > 0) {
    const snapshot = kafkaEngine.getSnapshot();
    const chunkerStatus = videoChunker.getStatus();
    broadcast('telemetry:snapshot', {
      kafka: snapshot,
      chunker: chunkerStatus
    });
  }
}, 500);

// ----------------------------------------------------
// REST API ENDPOINTS
// ----------------------------------------------------

app.get('/api/snapshot', (req, res) => {
  res.json({
    kafka: kafkaEngine.getSnapshot(),
    chunker: videoChunker.getStatus()
  });
});

app.post('/api/stream/start', (req, res) => {
  try {
    if (videoChunker.currentChunkIndex === 0) {
      resetReassemblyState(videoChunker.currentFilename, videoChunker.streamId);
    }
    const result = videoChunker.startStream();
    res.json({ success: true, result });
  } catch (err) {
    res.status(400).json({ success: false, error: err.message });
  }
});

app.post('/api/stream/pause', (req, res) => {
  const result = videoChunker.pauseStream();
  res.json({ success: true, result });
});

app.post('/api/stream/stop', (req, res) => {
  const result = videoChunker.stopStream();
  res.json({ success: true, result });
});

app.post('/api/stream/config', (req, res) => {
  const { chunkSize, speedInterval } = req.body;
  if (chunkSize) {
    videoChunker.setChunkSize(chunkSize);
  }
  if (speedInterval) {
    videoChunker.setSpeed(speedInterval);
  }
  res.json({ success: true, status: videoChunker.getStatus() });
});

app.post('/api/consumer/:groupId/pause', (req, res) => {
  const { groupId } = req.params;
  kafkaEngine.pauseConsumerGroup(groupId);
  res.json({ success: true, groupId, paused: true });
});

app.post('/api/consumer/:groupId/resume', (req, res) => {
  const { groupId } = req.params;
  kafkaEngine.resumeConsumerGroup(groupId);
  res.json({ success: true, groupId, paused: false });
});

app.post('/api/system/reset', (req, res) => {
  videoChunker.stopStream();
  resetReassemblyState(videoChunker.currentFilename, null);
  kafkaEngine.resetAll();
  broadcast('system:reset', {});
  res.json({ success: true });
});

// Custom video upload endpoint (base64)
app.post('/api/stream/upload', (req, res) => {
  const { filename, base64Content } = req.body;
  if (!base64Content) {
    return res.status(400).json({ success: false, error: 'No content provided' });
  }
  try {
    const buffer = Buffer.from(base64Content, 'base64');
    const result = videoChunker.setCustomVideo(buffer, filename || 'custom_upload.mp4');
    resetReassemblyState(filename || 'custom_upload.mp4', result.streamId);
    kafkaEngine.resetAll();
    broadcast('system:reset', {});
    res.json({ success: true, result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// WebSocket Connection Handshake
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({
    type: 'welcome',
    data: {
      message: 'Connected to Apache Kafka Video Streaming Bridge',
      snapshot: kafkaEngine.getSnapshot(),
      chunker: videoChunker.getStatus()
    }
  }));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`====================================================`);
  console.log(`  Apache Kafka Video Streaming & Chunking Demo`);
  console.log(`  Server running at: http://localhost:${PORT}`);
  console.log(`  WebSocket Bridge:  ws://localhost:${PORT}/ws`);
  console.log(`====================================================`);
});
