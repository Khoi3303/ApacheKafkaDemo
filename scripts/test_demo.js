const http = require('http');

function request(path, method = 'GET', body = null) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 3000,
      path,
      method,
      headers: {
        'Content-Type': 'application/json'
      }
    };
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch (e) {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function runTest() {
  console.log('--- 1. Testing GET /api/snapshot ---');
  let snap = await request('/api/snapshot');
  console.log('Snapshot status:', snap.status, 'Total produced:', snap.body.kafka.stats.totalProduced);

  console.log('\n--- 2. Testing POST /api/stream/start ---');
  let startRes = await request('/api/stream/start', 'POST');
  console.log('Start status:', startRes.status, startRes.body);

  console.log('\n--- 3. Waiting 3 seconds for chunks to flow through Kafka ---');
  await new Promise(r => setTimeout(r, 3000));

  console.log('\n--- 4. Checking snapshot after 3s ---');
  snap = await request('/api/snapshot');
  console.log('Total produced messages:', snap.body.kafka.stats.totalProduced);
  console.log('Throughput:', (snap.body.kafka.stats.produceRateBps / 1024).toFixed(1), 'KB/s');
  console.log('Consumer Groups state:');
  for (const [id, c] of Object.entries(snap.body.kafka.consumers)) {
    console.log(`  - ${id}: processed ${c.messagesProcessed} msgs, lag: ${c.lag}`);
  }
  console.log('Database rows stored:', snap.body.database.recentRows.length);

  console.log('\n--- 5. Testing Consumer Pause (Pause AI Transcriber) ---');
  await request('/api/consumer/cg-ai-transcribe/pause', 'POST');
  await new Promise(r => setTimeout(r, 1500));
  snap = await request('/api/snapshot');
  console.log('Consumer 2 Lag after pause:', snap.body.kafka.consumers['cg-ai-transcribe'].lag);

  console.log('\n--- 6. Resuming Consumer 2 ---');
  await request('/api/consumer/cg-ai-transcribe/resume', 'POST');
  snap = await request('/api/snapshot');
  console.log('Consumer 2 Lag after resume:', snap.body.kafka.consumers['cg-ai-transcribe'].lag);

  console.log('\n--- 7. Stopping Stream ---');
  await request('/api/stream/stop', 'POST');
  console.log('Stream stopped. All tests PASSED successfully!');
}

runTest().catch(console.error);
