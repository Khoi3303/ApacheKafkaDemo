/**
 * Apache Kafka Demo - Client Application Logic
 * Interactive WebSocket streaming, video chunk reassembly, animated topology, and Kafka internal metrics
 */

document.addEventListener('DOMContentLoaded', () => {
  // Application State
  const state = {
    ws: null,
    isConnected: false,
    isStreaming: false,
    isPaused: false,
    chunkSize: 65536,
    speedInterval: 250,
    videoChunks: [],
    videoBlobUrl: null,
    totalExpectedChunks: 0,
    currentChunkIndex: 0,
    consumersPaused: {
      'cg-video-streamer': false,
      'cg-msg-inspector': false,
      'cg-reassembly-tracker': false
    },
    particles: [],
    lastTelemetry: null,
    currentFilename: 'sample_video.mp4',
    chunkSizeKB: 64,
    speedInterval: 250
  };

  // DOM Elements - Producer Controls
  const wsStatusText = document.getElementById('ws-status-text');
  const btnStart = document.getElementById('btn-start');
  const btnStartIcon = document.getElementById('btn-start-icon');
  const btnStartText = document.getElementById('btn-start-text');
  const btnPause = document.getElementById('btn-pause');
  const btnPauseIcon = document.getElementById('btn-pause-icon');
  const btnPauseText = document.getElementById('btn-pause-text');
  const btnStop = document.getElementById('btn-stop');
  const btnReset = document.getElementById('btn-reset');
  const producerStatusBadge = document.getElementById('producer-status-badge');
  const selectChunkSize = document.getElementById('select-chunk-size');
  const selectSpeed = document.getElementById('select-speed');

  // DOM Elements - Key Metrics
  const valTotalMsgs = document.getElementById('val-total-msgs');
  const valChunksProgress = document.getElementById('val-chunks-progress');
  const valThroughput = document.getElementById('val-throughput');
  const valMsgsSec = document.getElementById('val-msgs-sec');
  const valTotalLag = document.getElementById('val-total-lag');
  const valLagStatus = document.getElementById('val-lag-status');

  // DOM Elements - Consumer 1: Video
  const videoPlayer = document.getElementById('lecture-video-player');
  const playerChunkStatus = document.getElementById('player-chunk-status');
  const playerBufferSize = document.getElementById('player-buffer-size');
  const playerProgressFill = document.getElementById('player-progress-fill');
  const c1Processed = document.getElementById('c1-processed');
  const c1Lag = document.getElementById('c1-lag');
  const c1LiveStatus = document.getElementById('c1-live-status');
  const c1StatusText = document.getElementById('c1-status-text');
  const btnToggleC1 = document.getElementById('btn-toggle-c1');

  // DOM Elements - Consumer 2: Kafka Message Inspector
  const msgInspectorTerminal = document.getElementById('msg-inspector-terminal');
  const c2Processed = document.getElementById('c2-processed');
  const c2Lag = document.getElementById('c2-lag');
  const c2LiveStatus = document.getElementById('c2-live-status');
  const c2StatusText = document.getElementById('c2-status-text');
  const btnToggleC2 = document.getElementById('btn-toggle-c2');

  // DOM Elements - Consumer 3: Chunk Reassembly Tracker
  const reassemblyTerminal = document.getElementById('reassembly-terminal');
  const reassemblyMapEl = document.getElementById('reassembly-map');
  const c3Processed = document.getElementById('c3-processed');
  const c3Lag = document.getElementById('c3-lag');
  const c3LiveStatus = document.getElementById('c3-live-status');
  const c3StatusText = document.getElementById('c3-status-text');
  const btnToggleC3 = document.getElementById('btn-toggle-c3');

  // Canvas & Partitions
  const partitionTrackContainer = document.getElementById('partition-track-container');
  const topologyCanvas = document.getElementById('topology-canvas');

  // Upload modal elements
  const btnOpenUpload = document.getElementById('btn-open-upload');
  const uploadModal = document.getElementById('upload-modal');
  const btnCloseModal = document.getElementById('btn-close-modal');
  const btnCancelModal = document.getElementById('btn-cancel-modal');
  const dropzone = document.getElementById('dropzone');
  const fileInput = document.getElementById('file-input');

  // Setup Canvas
  const ctx = topologyCanvas.getContext('2d');
  function resizeCanvas() {
    if (!topologyCanvas || !topologyCanvas.parentElement) return;
    const parentW = topologyCanvas.parentElement.clientWidth;
    // Account for padding (24px left + 24px right)
    topologyCanvas.width = Math.max(340, parentW - 48);
    topologyCanvas.height = 240;
  }
  window.addEventListener('resize', resizeCanvas);
  if (window.ResizeObserver && topologyCanvas.parentElement) {
    const ro = new ResizeObserver(() => resizeCanvas());
    ro.observe(topologyCanvas.parentElement);
  }
  resizeCanvas();

  // Toast Notification Helper
  function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = 'toast';
    const icon = type === 'error' ? '❌' : type === 'success' ? '✅' : '⚡';
    toast.innerHTML = `<span>${icon}</span> <span>${message}</span>`;
    container.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(100%)';
      setTimeout(() => toast.remove(), 300);
    }, 3500);
  }

  // Always use light theme - no toggle needed
  document.body.classList.add('theme-light');

  // ----------------------------------------------------
  // UNIFIED PRODUCER & CONSUMERS STATUS SYNC
  // ----------------------------------------------------
  function updateSystemStatusUI() {
    // 1. Update Start button — only shows when NOT streaming
    if (state.isStreaming) {
      if (btnStart) {
        btnStart.disabled = true;
        btnStart.className = 'btn btn-primary';
      }
      if (btnStartIcon) btnStartIcon.textContent = '▶';
      if (btnStartText) btnStartText.textContent = 'Bắt Đầu Stream';
      // Enable Pause button
      if (btnPause) btnPause.disabled = false;
      if (!state.isPaused) {
        if (btnPauseIcon) btnPauseIcon.textContent = '⏸';
        if (btnPauseText) btnPauseText.textContent = 'Tạm Dừng';
        if (btnPause) btnPause.className = 'btn btn-secondary';
        if (producerStatusBadge) {
          producerStatusBadge.className = 'status-badge streaming';
          const chunkKB = state.chunkSizeKB || 64;
          const msgsPerSec = Math.round(1000 / (state.speedInterval || 250));
          producerStatusBadge.textContent = `🟢 Đang phát — ${chunkKB}KB/chunk · ${msgsPerSec} chunks/giây`;
        }
      } else {
        if (btnPauseIcon) btnPauseIcon.textContent = '▶';
        if (btnPauseText) btnPauseText.textContent = 'Tiếp Tục';
        if (btnPause) btnPause.className = 'btn btn-primary';
        if (producerStatusBadge) {
          producerStatusBadge.className = 'status-badge paused';
          producerStatusBadge.textContent = '⏸ Nguồn phát đang tạm dừng';
        }
      }
    } else {
      if (btnStart) {
        btnStart.disabled = false;
        btnStart.className = 'btn btn-primary';
      }
      if (btnStartIcon) btnStartIcon.textContent = '▶';
      if (btnStartText) btnStartText.textContent = 'Bắt Đầu Stream';
      if (btnPause) { btnPause.disabled = true; btnPause.className = 'btn btn-secondary'; }
      if (btnPauseIcon) btnPauseIcon.textContent = '⏸';
      if (btnPauseText) btnPauseText.textContent = 'Tạm Dừng';
      if (producerStatusBadge) {
        producerStatusBadge.className = 'status-badge idle';
        producerStatusBadge.textContent = '⚪ Sẵn sàng phát';
      }
    }

    // 2. Synchronize all 3 Consumers with Producer state
    const consumers = [
      { id: 'cg-video-streamer', el: c1StatusText, box: c1LiveStatus, name: 'Consumer Video', btn: btnToggleC1 },
      { id: 'cg-msg-inspector', el: c2StatusText, box: c2LiveStatus, name: 'Message Inspector', btn: btnToggleC2 },
      { id: 'cg-reassembly-tracker', el: c3StatusText, box: c3LiveStatus, name: 'Reassembly Tracker', btn: btnToggleC3 }
    ];

    consumers.forEach(c => {
      if (!c.el || !c.box || !c.btn) return;
      const isDisconnected = state.consumersPaused[c.id];
      if (isDisconnected) {
        c.el.textContent = '🔴 ĐÃ NGẮT KẾT NỐI (Consumer Lag đang tăng!)';
        c.box.className = 'consumer-live-status paused';
        c.btn.textContent = '🟢 Kết nối lại (Đọc bù Offset)';
        c.btn.classList.add('paused');
      } else {
        c.btn.textContent = `🔴 Ngắt ${c.name} (xem Lag tăng)`;
        c.btn.classList.remove('paused');

        if (state.isStreaming && !state.isPaused) {
          c.el.textContent = '🟢 Đang nhận tin nhắn từ Kafka';
          c.box.className = 'consumer-live-status';
        } else if (state.isStreaming && state.isPaused) {
          c.el.textContent = '⏸ Chờ chunk mới (Nguồn phát đang tạm dừng)';
          c.box.className = 'consumer-live-status waiting';
        } else {
          c.el.textContent = '⚪ Sẵn sàng nhận dữ liệu từ Kafka';
          c.box.className = 'consumer-live-status waiting';
        }
      }
    });
  }

  // ----------------------------------------------------
  // WEBSOCKET CONNECTION
  // ----------------------------------------------------
  function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    state.ws = new WebSocket(wsUrl);

    state.ws.onopen = () => {
      state.isConnected = true;
      wsStatusText.textContent = 'Kafka Broker Đã Kết Nối';
      wsStatusText.parentElement.style.borderColor = 'rgba(16, 185, 129, 0.4)';
    };

    state.ws.onclose = () => {
      state.isConnected = false;
      wsStatusText.textContent = 'Mất kết nối Broker (Đang thử lại...)';
      wsStatusText.parentElement.style.borderColor = 'rgba(244, 63, 94, 0.4)';
      setTimeout(initWebSocket, 2000);
    };

    state.ws.onerror = (err) => {
      console.error('WebSocket Error:', err);
    };

    state.ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        handleIncomingMessage(msg);
      } catch (e) {
        console.error('Error parsing WS message:', e);
      }
    };
  }

  // ----------------------------------------------------
  // INCOMING MESSAGE HANDLERS
  // ----------------------------------------------------
  function handleIncomingMessage(msg) {
    switch (msg.type) {
      case 'kafka:produced':
        spawnCanvasParticle(msg.data);
        break;

      case 'consumer:video-stream':
        handleVideoChunk(msg.data);
        break;

      case 'consumer:msg-inspector':
        handleMsgInspector(msg.data);
        break;

      case 'consumer:reassembly-tracker':
        handleReassemblyTracker(msg.data);
        break;

      case 'telemetry:snapshot':
        updateTelemetry(msg.data);
        break;

      case 'stream:completed':
        state.isStreaming = false;
        state.isPaused = false;
        updateSystemStatusUI();
        showToast('Truyền phát hoàn tất! Tất cả chunks đã được Kafka phát tán thành công.', 'success');
        break;

      case 'system:reset':
        resetLocalState();
        showToast('Đã reset toàn bộ Kafka Topic, Offsets và Consumer Groups.', 'info');
        break;

      // Real Kafka consumer pause/resume events
      case 'consumer:paused':
        state.consumersPaused[msg.data.groupId] = true;
        updateSystemStatusUI();
        break;

      case 'consumer:resumed':
        state.consumersPaused[msg.data.groupId] = false;
        updateSystemStatusUI();
        break;
    }
  }

  // Consumer 1: Video Chunk Reassembly & HTML5 Player
  function handleVideoChunk(data) {
    state.totalExpectedChunks = data.totalChunks;
    state.currentChunkIndex = data.chunkIndex + 1;

    // Reset buffer immediately when a new stream/video begins
    // Reset buffer immediately when a new stream/video begins
    const overlay = document.querySelector('.video-status-overlay');
    if (data.chunkIndex === 0 || (state.currentStreamId && state.currentStreamId !== data.streamId)) {
      state.videoChunks = [];
      state.currentStreamId = data.streamId;
      if (overlay) overlay.style.opacity = '1';
      if (state.videoBlobUrl) {
        URL.revokeObjectURL(state.videoBlobUrl);
        state.videoBlobUrl = null;
      }
      videoPlayer.pause();
      videoPlayer.removeAttribute('src');
      videoPlayer.load();
    }

    // Convert base64 to binary ArrayBuffer chunk
    const byteCharacters = atob(data.base64Data);
    const byteNumbers = new Uint8Array(byteCharacters.length);
    for (let i = 0; i < byteCharacters.length; i++) {
      byteNumbers[i] = byteCharacters.charCodeAt(i);
    }
    state.videoChunks.push(byteNumbers);

    // Calculate total buffered bytes
    const totalBytes = state.videoChunks.reduce((acc, c) => acc + c.length, 0);
    const progress = Math.round((state.currentChunkIndex / data.totalChunks) * 100);
    const isCompleted = state.currentChunkIndex >= data.totalChunks;

      if (!isCompleted) {
      // While streaming: Show buffering progress — do NOT touch video element
      if (overlay) overlay.style.opacity = '1';
      playerChunkStatus.textContent = `⏳ Đang đệm chunk #${data.chunkIndex + 1}/${data.totalChunks} từ Kafka [Offset: #${data.offset}]`;
      playerBufferSize.textContent = `Buffer: ${(totalBytes / 1024).toFixed(1)} KB (${progress}%)`;
      playerProgressFill.style.width = `${progress}%`;
    } else {
      // All chunks received from Kafka: reassemble the full MP4 blob and play
      playerChunkStatus.textContent = `✅ Tái ghép đủ ${data.totalChunks}/${data.totalChunks} chunks từ Kafka — Đang phát`;
      playerBufferSize.textContent = `Buffer: ${(totalBytes / 1024).toFixed(1)} KB (100%)`;
      playerProgressFill.style.width = '100%';

      try {
        const mime = data.mimeType || 'video/mp4';
        const fullBlob = new Blob(state.videoChunks, { type: mime });
        if (state.videoBlobUrl) URL.revokeObjectURL(state.videoBlobUrl);
        state.videoBlobUrl = URL.createObjectURL(fullBlob);

        videoPlayer.src = state.videoBlobUrl;
        videoPlayer.currentTime = 0;
        videoPlayer.play().catch(() => {});

        // Auto-fade status overlay after 3 seconds so the full video is unobstructed
        if (overlay) {
          setTimeout(() => {
            if (overlay && state.isStreaming === false) {
              overlay.style.opacity = '0';
            }
          }, 3200);
        }
      } catch (err) {
        console.error('Blob assembly error:', err);
      }
    }
  }

  // Consumer 2: Kafka Message Inspector — real partition from Apache Kafka broker
  function handleMsgInspector(data) {
    const pColors = ['#0ea5e9', '#a855f7', '#10b981'];
    const pColor = pColors[data.assignedPartition] || '#94a3b8';
    const isCatchUp = data.isCatchUp ? ' <span style="color:#f59e0b;font-size:10px">[CATCH-UP]</span>' : '';

    const entry = document.createElement('div');
    entry.className = 'inspector-entry';
    entry.innerHTML = `
      <div class="inspector-header">
        <span class="inspector-chunk-id">CHUNK #${data.chunkIndex} / ${data.totalChunks - 1}</span>
        <span class="inspector-partition" style="background:${pColor}20;color:${pColor};border:1px solid ${pColor}40">→ Partition ${data.assignedPartition}${isCatchUp}</span>
        <span class="inspector-offset">Offset: <strong>#${data.offset}</strong></span>
      </div>
      <div class="inspector-routing">
        <span class="inspector-label">KEY</span>
        <code class="inspector-code">${data.partitionKey}</code>
        <span class="inspector-arrow">→</span>
        <span class="inspector-label">TOPIC</span>
        <code class="inspector-code">video-stream</code>
        <span class="inspector-arrow">→</span>
        <span class="inspector-label">PARTITION</span>
        <code class="inspector-code" style="color:${pColor}">P${data.assignedPartition} ✓ (từ Kafka Broker)</code>
      </div>
      <div class="inspector-bytes">
        <span class="inspector-label">BYTES</span>
        <code class="inspector-code">[${(data.startByte || 0).toLocaleString()} → ${(data.endByte || 0).toLocaleString()}]</code>
        <span class="inspector-label" style="margin-left:8px">SIZE</span>
        <code class="inspector-code">${((data.chunkSize || 0) / 1024).toFixed(1)} KB</code>
        <span class="inspector-label" style="margin-left:8px">MD5</span>
        <code class="inspector-code">${data.checksum || '-'}</code>
      </div>
      <div class="inspector-hex">
        <span class="inspector-label">HEX PREVIEW (12 bytes đầu)</span>
        <code class="inspector-hex-data">${data.hexPreview || ''}</code>
      </div>
    `;
    msgInspectorTerminal.prepend(entry);
    // Keep only last 15 entries
    while (msgInspectorTerminal.children.length > 15) {
      msgInspectorTerminal.removeChild(msgInspectorTerminal.lastChild);
    }
  }

  // Consumer 3: Chunk Reassembly Tracker — shows accumulation progress and partition map
  function handleReassemblyTracker(data) {
    // Update the chunk map display
    if (reassemblyMapEl) {
      const pct = Math.round((data.receivedCount / data.totalChunks) * 100);
      const assembledKB = (data.totalBytesAssembled / 1024).toFixed(1);
      const totalKB = (data.totalBytes / 1024).toFixed(1);

      reassemblyMapEl.innerHTML = `
        <div class="reassembly-progress-header">
          <span>Tái ghép: <strong>${assembledKB} KB</strong> / ${totalKB} KB (${pct}%)</span>
          <span style="color:#10b981">Nhận: ${data.receivedCount}/${data.totalChunks} chunks</span>
        </div>
        <div class="chunk-map-visual">
          ${data.chunkMap.split('').map((c, i) => {
            const chunkInfo = (state.lastReassemblyData && state.lastReassemblyData.chunkIndex === i)
              ? ` title="Chunk #${i}"` : ` title="Chunk #${i}"`;
            const pIdx = i % 3;
            const pColorMap = ['#0ea5e9','#a855f7','#10b981'];
            const col = c === '■' ? pColorMap[pIdx] : '#334155';
            return `<span class="chunk-cell" style="background:${col}" title="Chunk #${i} → P${pIdx}"></span>`;
          }).join('')}
        </div>
        <div class="reassembly-partition-dist">
          ${data.partitionDistribution.map((d, i) => {
            const pColorMap = ['#0ea5e9','#a855f7','#10b981'];
            return `<span style="color:${pColorMap[i]};font-family:var(--font-mono);font-size:12px">${d}</span>`;
          }).join('<span style="color:#475569;margin:0 6px">|</span>')}
        </div>
      `;
    }

    // Log entry per chunk
    const entry = document.createElement('div');
    entry.className = 'reassembly-log-entry';
    const pColorMap = ['#0ea5e9', '#a855f7', '#10b981'];
    const pCol = pColorMap[data.partition] || '#94a3b8';
    entry.innerHTML = `
      <span style="color:${pCol};font-weight:700">P${data.partition}:#${data.offset}</span>
      <span style="color:#64748b;margin:0 6px">chunk[${data.chunkIndex}]</span>
      <span style="color:#94a3b8">[${data.startByte.toLocaleString()}→${data.endByte.toLocaleString()}]</span>
      <span style="color:#475569;margin:0 4px">+${(data.chunkSize/1024).toFixed(0)}KB</span>
      <span style="color:#10b981;margin-left:auto">✓ ghép vào buffer</span>
    `;
    reassemblyTerminal.prepend(entry);
    while (reassemblyTerminal.children.length > 20) {
      reassemblyTerminal.removeChild(reassemblyTerminal.lastChild);
    }

    state.lastReassemblyData = data;
  }

  // Update Telemetry Metrics Bar & Partitions
  function updateTelemetry(data) {
    state.lastTelemetry = data;
    const { kafka, chunker } = data;

    // Sync streaming state if chunker changed
    state.isStreaming = chunker.isStreaming;
    state.isPaused = chunker.isPaused;
    if (chunker.currentFilename) state.currentFilename = chunker.currentFilename;
    if (chunker.chunkSize) state.chunkSizeKB = Math.round(chunker.chunkSize / 1024);
    if (chunker.streamingInterval) state.speedInterval = chunker.streamingInterval;
    updateSystemStatusUI();

    // Metrics Bar
    valTotalMsgs.textContent = kafka.stats.totalProduced;
    valChunksProgress.textContent = `${chunker.currentChunkIndex} / ${chunker.totalChunks} chunks (${chunker.progressPercent}%)`;
    valThroughput.textContent = `${(kafka.stats.produceRateBps / 1024).toFixed(1)} KB/s`;
    valMsgsSec.textContent = `${kafka.stats.messagesPerSec} msgs/sec`;

    // Consumer Groups stats — updated group IDs
    const c1 = kafka.consumers['cg-video-streamer'];
    const c2 = kafka.consumers['cg-msg-inspector'];
    const c3 = kafka.consumers['cg-reassembly-tracker'];

    let totalLag = 0;
    if (c1) {
      c1Processed.textContent = c1.messagesProcessed;
      c1Lag.textContent = `Lag: ${c1.lag}`;
      c1Lag.className = `lag-indicator ${c1.lag > 10 ? 'danger' : c1.lag > 0 ? 'warning' : 'zero'}`;
      totalLag += c1.lag;
    }
    if (c2) {
      c2Processed.textContent = c2.messagesProcessed;
      c2Lag.textContent = `Lag: ${c2.lag}`;
      c2Lag.className = `lag-indicator ${c2.lag > 10 ? 'danger' : c2.lag > 0 ? 'warning' : 'zero'}`;
      totalLag += c2.lag;
    }
    if (c3) {
      c3Processed.textContent = c3.messagesProcessed;
      c3Lag.textContent = `Lag: ${c3.lag}`;
      c3Lag.className = `lag-indicator ${c3.lag > 10 ? 'danger' : c3.lag > 0 ? 'warning' : 'zero'}`;
      totalLag += c3.lag;
    }

    valTotalLag.textContent = totalLag;
    valLagStatus.textContent = totalLag === 0 ? 'Đồng bộ hoàn hảo' : `Đang chậm ${totalLag} offsets`;
    valLagStatus.style.color = totalLag === 0 ? 'var(--accent-emerald)' : 'var(--accent-amber)';

    // Update Partition Tracks
    renderPartitionTracks(kafka);
  }

  // Render Visual Partition Logs
  function renderPartitionTracks(kafka) {
    const topic = kafka.topics['video-stream'] || (kafka.topics && Object.values(kafka.topics)[0]);
    if (!topic) return;

    let html = '';
    topic.partitions.forEach(p => {
      html += `
        <div class="partition-row">
          <div class="partition-label">
            <span>⚡ Partition ${p.partitionId}</span>
          </div>
          <div class="partition-log-slots">
      `;

      const startOffset = Math.max(0, p.latestOffset - 7);
      const endOffset = p.latestOffset;

      if (p.messageCount === 0) {
        html += `<div style="font-size: 11px; color: var(--text-muted); padding: 12px;">(Chưa có message)</div>`;
      } else {
        for (let off = startOffset; off <= endOffset; off++) {
          const isLatest = off === endOffset;
          const slotKB = state.chunkSizeKB || 64;
          html += `
            <div class="offset-slot ${isLatest ? 'active' : ''}">
              <div class="offset-slot-num">#${off}</div>
              <div class="offset-slot-tag">${slotKB}KB</div>
              <div class="consumer-cursors">
                <span class="cursor-dot c1" title="CG-1: Video Player"></span>
                <span class="cursor-dot c2" title="CG-2: Inspector"></span>
                <span class="cursor-dot c3" title="CG-3: Reassembly"></span>
              </div>
            </div>
          `;
        }
      }

      html += `
          </div>
          <div style="font-family: var(--font-mono); font-size: 12px; color: var(--text-secondary); width: 110px; text-align: right;">
            Latest: #${p.latestOffset >= 0 ? p.latestOffset : 0}
          </div>
        </div>
      `;
    });

    partitionTrackContainer.innerHTML = html;
  }

  // ----------------------------------------------------
  // ANIMATED TOPOLOGY PIPELINE CANVAS
  // ----------------------------------------------------
  function spawnCanvasParticle(data) {
    // data from real Kafka: { topic, partition, offset, key, size, timestamp }
    state.particles.push({
      id: Math.random(),
      chunkIndex: data.offset || 0,
      partition: data.partition || 0,
      progress: 0,
      speed: 0.015,
      color: '#ea580c'
    });
  }

  function drawTopology() {
    ctx.clearRect(0, 0, topologyCanvas.width, topologyCanvas.height);
    const w = topologyCanvas.width;
    const h = topologyCanvas.height;
    const isLight = document.body.classList.contains('theme-light');

    if (isLight) {
      ctx.fillStyle = '#f8fafc';
      ctx.fillRect(0, 0, w, h);
    }

    // DYNAMIC RESPONSIVE COLUMNS (Generous fan-out space for Broker -> Consumers)
    const margin = Math.max(14, Math.min(32, w * 0.03));
    const colWidth = Math.max(82, Math.min(136, Math.floor((w - 2 * margin) / 4.6)));
    const consumerWidth = Math.max(90, Math.min(146, Math.floor((w - 2 * margin) / 4.3)));

    const xSource = Math.round(margin + colWidth / 2);
    const xChunker = Math.round(w * 0.30);
    const xBroker = Math.round(w * 0.53);
    const xConsumers = Math.round(w - margin - consumerWidth / 2);

    // Node Positions with high contrast theme-aware colors
    const nodeSource = { 
      x: xSource, 
      y: h / 2, 
      label: 'Video Nguồn Phát', 
      sub: '',
      color: '#0284c7'
    };
    const nodeChunker = { 
      x: xChunker, 
      y: h / 2, 
      label: 'Chunker / Producer', 
      sub: `Băm ${state.chunkSizeKB || 64}KB/chunk`,
      color: '#ea580c'
    };
    const nodeBroker = { 
      x: xBroker, 
      y: h / 2, 
      label: 'Apache Kafka Broker', 
      sub: '',
      color: '#ea580c'
    };

    const nodeC1 = { 
      x: xConsumers, 
      y: 55, 
      label: state.consumersPaused['cg-video-streamer'] ? 'Consumer 1: [ĐÃ NGẮT]' : 'Consumer 1: Player', 
      sub: state.consumersPaused['cg-video-streamer'] ? '🔴 Lag dồn ứ' : 'cg-video-streamer', 
      color: state.consumersPaused['cg-video-streamer'] ? '#ef4444' : (isLight ? '#0284c7' : '#00f2fe'),
      isDisconnected: !!state.consumersPaused['cg-video-streamer']
    };
    const nodeC2 = { 
      x: xConsumers, 
      y: h / 2, 
      label: state.consumersPaused['cg-msg-inspector'] ? 'Consumer 2: [NGẮT]' : 'Consumer 2: Inspector', 
      sub: state.consumersPaused['cg-msg-inspector'] ? '🔴 Lag dồn ứ' : 'cg-msg-inspector', 
      color: state.consumersPaused['cg-msg-inspector'] ? '#ef4444' : (isLight ? '#7c3aed' : '#a855f7'),
      isDisconnected: !!state.consumersPaused['cg-msg-inspector']
    };
    const nodeC3 = { 
      x: xConsumers, 
      y: h - 55, 
      label: state.consumersPaused['cg-reassembly-tracker'] ? 'Consumer 3: [NGẮT]' : 'Consumer 3: Reassembly', 
      sub: state.consumersPaused['cg-reassembly-tracker'] ? '🔴 Lag dồn ứ' : 'cg-reassembly-tracker', 
      color: state.consumersPaused['cg-reassembly-tracker'] ? '#ef4444' : (isLight ? '#059669' : '#10b981'),
      isDisconnected: !!state.consumersPaused['cg-reassembly-tracker']
    };

    // Draw connection lines
    function drawLine(p1, p2, color = null, isDotted = false) {
      ctx.beginPath();
      ctx.strokeStyle = color || (isLight ? '#cbd5e1' : 'rgba(255, 255, 255, 0.12)');
      ctx.lineWidth = isLight ? 2.5 : 2;
      if (isDotted) ctx.setLineDash([6, 6]);
      else ctx.setLineDash([]);
      ctx.moveTo(p1.x, p1.y);
      ctx.lineTo(p2.x, p2.y);
      ctx.stroke();
      ctx.setLineDash([]);
    }

    // Connections
    drawLine(nodeSource, nodeChunker);
    drawLine(nodeChunker, nodeBroker);
    
    // Broadcast Fan-out connections
    drawLine(
      nodeBroker, 
      nodeC1, 
      nodeC1.isDisconnected ? (isLight ? '#fca5a5' : 'rgba(239, 68, 68, 0.4)') : (isLight ? 'rgba(2, 132, 199, 0.45)' : 'rgba(0, 242, 254, 0.25)'),
      nodeC1.isDisconnected
    );
    drawLine(
      nodeBroker, 
      nodeC2, 
      nodeC2.isDisconnected ? (isLight ? '#fca5a5' : 'rgba(239, 68, 68, 0.4)') : (isLight ? 'rgba(124, 58, 237, 0.45)' : 'rgba(168, 85, 247, 0.25)'),
      nodeC2.isDisconnected
    );
    drawLine(
      nodeBroker, 
      nodeC3, 
      nodeC3.isDisconnected ? (isLight ? '#fca5a5' : 'rgba(239, 68, 68, 0.4)') : (isLight ? 'rgba(5, 150, 105, 0.45)' : 'rgba(16, 185, 129, 0.25)'),
      nodeC3.isDisconnected
    );

    // Draw Nodes with Perfect Centering & Dynamic Font Sizes
    function drawNode(node, width = 140, height = 54) {
      ctx.fillStyle = node.isDisconnected 
        ? (isLight ? '#fef2f2' : 'rgba(239, 68, 68, 0.15)')
        : (isLight ? '#ffffff' : 'rgba(14, 21, 38, 0.9)');
      ctx.strokeStyle = node.color;
      ctx.lineWidth = isLight ? 2.5 : 1.5;
      const x = node.x - width / 2;
      const y = node.y - height / 2;

      ctx.beginPath();
      if (node.isDisconnected) {
        ctx.setLineDash([4, 4]);
      } else {
        ctx.setLineDash([]);
      }
      ctx.roundRect(x, y, width, height, 8);
      ctx.fill();
      ctx.stroke();
      ctx.setLineDash([]);

      const hasSub = node.sub && node.sub.trim().length > 0;
      const fontSize = (w < 650 || width < 110) ? '11px' : '12px';
      ctx.fillStyle = node.isDisconnected ? '#dc2626' : (isLight ? '#0f172a' : '#ffffff');
      ctx.font = isLight ? `700 ${fontSize} Outfit, sans-serif` : `600 ${fontSize} Outfit, sans-serif`;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';

      if (hasSub) {
        // Two-line layout: Title on top, Subtitle on bottom
        ctx.fillText(node.label, node.x, node.y - 7);
        ctx.fillStyle = node.color;
        const subFontSize = (w < 650 || width < 110) ? '9px' : '10px';
        ctx.font = isLight ? `700 ${subFontSize} JetBrains Mono, monospace` : `500 ${subFontSize} JetBrains Mono, monospace`;
        ctx.fillText(node.sub, node.x, node.y + 10);
      } else {
        // Auto-wrap into 2 lines if text is wider than the box
        const measured = ctx.measureText(node.label).width;
        if (measured > width - 12) {
          if (node.label === 'Apache Kafka Broker') {
            ctx.fillText('Apache Kafka', node.x, node.y - 7);
            ctx.fillText('Broker', node.x, node.y + 8);
          } else if (node.label === 'Video Nguồn Phát') {
            ctx.fillText('Video', node.x, node.y - 7);
            ctx.fillText('Nguồn Phát', node.x, node.y + 8);
          } else {
            ctx.fillText(node.label, node.x, node.y);
          }
        } else {
          // Single-line: EXACT VERTICAL & HORIZONTAL CENTER!
          ctx.fillText(node.label, node.x, node.y);
        }
      }
    }

    drawNode(nodeSource, colWidth, 50);
    drawNode(nodeChunker, colWidth, 54);
    drawNode(nodeBroker, colWidth, 54);
    drawNode(nodeC1, consumerWidth, 48);
    drawNode(nodeC2, consumerWidth, 48);
    drawNode(nodeC3, consumerWidth, 48);

    // Broadcast label in center (only if gap is large enough)
    const fanoutGap = (xConsumers - consumerWidth / 2) - (xBroker + colWidth / 2);
    if (fanoutGap > 70) {
      ctx.fillStyle = isLight ? '#334155' : 'rgba(255, 255, 255, 0.4)';
      ctx.font = '800 10px JetBrains Mono';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      const labelText = fanoutGap > 120 ? 'PUB/SUB BROADCAST (FAN-OUT)' : 'PUB/SUB FAN-OUT';
      ctx.fillText(labelText, (xBroker + xConsumers) / 2, h / 2 - 16);
    }

    // Update & draw flowing animated particles
    for (let i = state.particles.length - 1; i >= 0; i--) {
      const p = state.particles[i];
      p.progress += p.speed;

      if (p.progress >= 1) {
        state.particles.splice(i, 1);
        continue;
      }

      // 3 Stages of transit
      let currentX, currentY;
      if (p.progress < 0.3) {
        // Source to Chunker
        const t = p.progress / 0.3;
        currentX = nodeSource.x + t * (nodeChunker.x - nodeSource.x);
        currentY = nodeSource.y;
      } else if (p.progress < 0.6) {
        // Chunker to Broker
        const t = (p.progress - 0.3) / 0.3;
        currentX = nodeChunker.x + t * (nodeBroker.x - nodeChunker.x);
        currentY = nodeChunker.y;
      } else {
        // Broker to the Connected Consumers ONLY!
        const t = (p.progress - 0.6) / 0.4;
        currentX = nodeBroker.x + t * (nodeC2.x - nodeBroker.x);
        
        // Filter ONLY ACTIVE consumers! Do NOT send particles to disconnected consumers!
        const activeTargets = [nodeC1, nodeC2, nodeC3].filter(n => !n.isDisconnected);

        activeTargets.forEach(targetNode => {
          const branchY = nodeBroker.y + t * (targetNode.y - nodeBroker.y);
          ctx.beginPath();
          ctx.arc(currentX, branchY, isLight ? 6 : 4, 0, Math.PI * 2);
          ctx.fillStyle = targetNode.color;
          ctx.shadowColor = targetNode.color;
          ctx.shadowBlur = isLight ? 12 : 8;
          ctx.fill();
          ctx.shadowBlur = 0;
        });

        // If any consumer is disconnected, show backlog pulse at Broker
        const hasDisconnected = [nodeC1, nodeC2, nodeC3].some(n => n.isDisconnected);
        if (hasDisconnected) {
          ctx.beginPath();
          ctx.arc(nodeBroker.x + 80, nodeBroker.y, 4, 0, Math.PI * 2);
          ctx.fillStyle = '#ef4444';
          ctx.shadowColor = '#ef4444';
          ctx.shadowBlur = 10;
          ctx.fill();
          ctx.shadowBlur = 0;
        }

        continue;
      }

      // Draw single particle before branch
      ctx.beginPath();
      ctx.arc(currentX, currentY, isLight ? 7 : 5, 0, Math.PI * 2);
      ctx.fillStyle = isLight ? '#ea580c' : '#ff6b35';
      ctx.shadowColor = isLight ? '#ea580c' : '#ff6b35';
      ctx.shadowBlur = isLight ? 14 : 10;
      ctx.fill();
      ctx.shadowBlur = 0;
    }

    requestAnimationFrame(drawTopology);
  }

  requestAnimationFrame(drawTopology);

  // ----------------------------------------------------
  // SMART PRODUCER CONTROLS (Link logic)
  // ----------------------------------------------------
  btnStart.addEventListener('click', async () => {
    try {
      if (!state.isStreaming) {
        const res = await fetch('/api/stream/start', { method: 'POST' });
        const data = await res.json();
        if (data.success) {
          state.isStreaming = true;
          state.isPaused = false;
          updateSystemStatusUI();
          showToast('Bắt đầu truyền phát chunks vào Kafka Topic!', 'success');
        }
      }
    } catch (e) {
      showToast('Lỗi khi điều khiển stream: ' + e.message, 'error');
    }
  });

  if (btnPause) {
    btnPause.addEventListener('click', async () => {
      try {
        if (!state.isPaused) {
          const res = await fetch('/api/stream/pause', { method: 'POST' });
          const data = await res.json();
          if (data.success) {
            state.isPaused = true;
            updateSystemStatusUI();
            showToast('Tạm dừng Nguồn Phát. Consumer đang chờ chunk mới…', 'info');
          }
        } else {
          const res = await fetch('/api/stream/start', { method: 'POST' });
          const data = await res.json();
          if (data.success) {
            state.isPaused = false;
            updateSystemStatusUI();
            showToast('Tiếp tục phát tin nhắn vào Kafka!', 'success');
          }
        }
      } catch (e) {
        showToast('Lỗi: ' + e.message, 'error');
      }
    });
  }

  btnStop.addEventListener('click', async () => {
    try {
      await fetch('/api/stream/stop', { method: 'POST' });
      state.isStreaming = false;
      state.isPaused = false;
      updateSystemStatusUI();
      showToast('Đã dừng stream hoàn toàn.', 'info');
    } catch (e) {
      console.error(e);
    }
  });

  btnReset.addEventListener('click', async () => {
    if (confirm('Đặt lại toàn bộ Kafka Broker, Partitions, Offsets và Cơ sở dữ liệu?')) {
      await fetch('/api/system/reset', { method: 'POST' });
    }
  });

  function resetLocalState() {
    state.videoChunks = [];
    state.currentChunkIndex = 0;
    state.currentStreamId = null;
    state.lastReassemblyData = null;
    state.isStreaming = false;
    state.isPaused = false;
    if (state.videoBlobUrl) {
      URL.revokeObjectURL(state.videoBlobUrl);
      state.videoBlobUrl = null;
    }
    videoPlayer.pause();
    videoPlayer.removeAttribute('src');
    videoPlayer.load();
    playerChunkStatus.textContent = 'Chờ chunks từ Kafka...';
    playerBufferSize.textContent = 'Buffer: 0 KB';
    playerProgressFill.style.width = '0%';
    const overlay = document.querySelector('.video-status-overlay');
    if (overlay) overlay.style.opacity = '1';
    if (msgInspectorTerminal) msgInspectorTerminal.innerHTML = '';
    if (reassemblyTerminal) reassemblyTerminal.innerHTML = '';
    if (reassemblyMapEl) reassemblyMapEl.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:20px;font-size:13px">Chờ stream bắt đầu...</div>';
    updateSystemStatusUI();
  }

  // Config selectors
  selectChunkSize.addEventListener('change', async (e) => {
    state.chunkSize = parseInt(e.target.value, 10);
    await fetch('/api/stream/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chunkSize: state.chunkSize })
    });
    showToast(`Đã đổi kích thước Chunk thành: ${state.chunkSize / 1024} KB`, 'info');
  });

  selectSpeed.addEventListener('change', async (e) => {
    state.speedInterval = parseInt(e.target.value, 10);
    await fetch('/api/stream/config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ speedInterval: state.speedInterval })
    });
    showToast(`Đã đổi tốc độ stream thành interval ${state.speedInterval}ms`, 'info');
  });

  // Toggle Individual Consumer Groups (To show Consumer Lag buildup!)
  function setupConsumerToggle(btn, groupId, consumerName) {
    if (!btn) return;
    btn.addEventListener('click', async () => {
      const isCurrentlyPaused = state.consumersPaused[groupId];
      const endpoint = isCurrentlyPaused ? `/api/consumer/${groupId}/resume` : `/api/consumer/${groupId}/pause`;

      const res = await fetch(endpoint, { method: 'POST' });
      const data = await res.json();
      if (data.success) {
        state.consumersPaused[groupId] = !isCurrentlyPaused;
        updateSystemStatusUI();
        if (state.consumersPaused[groupId]) {
          showToast(`Đã ngắt kết nối ${consumerName}! Consumer Lag tăng dần — dữ liệu dồn ứ trong Kafka Partition Log!`, 'info');
        } else {
          showToast(`Đã kết nối lại ${consumerName}! Kafka replay offset bị lỡ (Catch-up từ committed offset)`, 'success');
        }
      }
    });
  }

  setupConsumerToggle(btnToggleC1, 'cg-video-streamer', 'Consumer 1 (Video Player)');
  setupConsumerToggle(btnToggleC2, 'cg-msg-inspector', 'Consumer 2 (Message Inspector)');
  setupConsumerToggle(btnToggleC3, 'cg-reassembly-tracker', 'Consumer 3 (Reassembly Tracker)');

  // Upload Custom Video Modal
  btnOpenUpload.addEventListener('click', () => uploadModal.classList.add('open'));
  btnCloseModal.addEventListener('click', () => uploadModal.classList.remove('open'));
  btnCancelModal.addEventListener('click', () => uploadModal.classList.remove('open'));
  dropzone.addEventListener('click', () => fileInput.click());

  fileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (file) handleFileUpload(file);
  });

  dropzone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropzone.classList.add('dragover');
  });

  dropzone.addEventListener('dragleave', () => {
    dropzone.classList.remove('dragover');
  });

  dropzone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropzone.classList.remove('dragover');
    const file = e.dataTransfer.files[0];
    if (file) handleFileUpload(file);
  });

  function handleFileUpload(file) {
    if (file.size > 50 * 1024 * 1024) {
      alert('Vui lòng chọn file dưới 50MB để demo mượt mà trong bộ nhớ.');
      return;
    }

    showToast(`Đang đọc file "${file.name}" (${(file.size / (1024 * 1024)).toFixed(2)} MB)...`, 'info');

    const reader = new FileReader();
    reader.onload = async () => {
      const base64Data = reader.result.split(',')[1];
      try {
        const res = await fetch('/api/stream/upload', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            filename: file.name,
            base64Content: base64Data
          })
        });
        const result = await res.json();
        if (result.success) {
          uploadModal.classList.remove('open');
          resetLocalState();
          state.currentFilename = file.name;
          const infoEl = document.getElementById('current-stream-info');
          if (infoEl) infoEl.textContent = `Tệp: ${file.name} (${(file.size / 1024).toFixed(0)} KB)`;
          showToast(`Đã nạp video "${file.name}". Bấm "Bắt Đầu Stream" để băm nhỏ vào Kafka!`, 'success');
        }
      } catch (err) {
        showToast('Lỗi khi tải file: ' + err.message, 'error');
      }
    };
    reader.readAsDataURL(file);
  }

  // Initial UI sync
  updateSystemStatusUI();

  // Start WebSocket
  initWebSocket();
});
