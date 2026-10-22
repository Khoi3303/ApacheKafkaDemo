const { EventEmitter } = require('events');
const { Kafka } = require('kafkajs');

const TOPIC = 'video-stream';
const PARTITIONS = 3;

const CONSUMER_GROUPS = [
  'cg-video-streamer',
  'cg-msg-inspector',
  'cg-reassembly-tracker'
];

class KafkaEngine extends EventEmitter {
  constructor() {
    super();

    this.kafka = null;
    this.admin = null;
    this.producer = null;

    this.ready = null;
    this.topicReady = false;
    this.mode = 'connecting';

    this.consumerGroups = new Map();

    this.stats = {
      totalProduced: 0,
      totalBytesProduced: 0,
      produceRateBps: 0,
      messagesPerSec: 0,
      startTime: Date.now()
    };

    this.recentBytes = [];
    this.snapshot = this.createEmptySnapshot();

    this.ready = this.init();

    setInterval(() => {
      this.refreshSnapshot().catch(() => { });
    }, 1000);
  }

  createEmptySnapshot() {
    return {
      mode: this.mode,
      stats: this.stats,
      topics: {
        [TOPIC]: {
          name: TOPIC,
          numPartitions: PARTITIONS,
          totalMessages: 0,
          partitions: Array.from(
            { length: PARTITIONS },
            (_, partitionId) => ({
              partitionId,
              messageCount: 0,
              latestOffset: -1,
              recentMessages: []
            })
          )
        }
      },
      consumers: {}
    };
  }

  async init() {
    const brokers = (
      process.env.KAFKA_BROKERS || 'localhost:9092'
    ).split(',').map(x => x.trim());

    this.kafka = new Kafka({
      clientId: 'apache-kafka-video-demo',
      brokers,
      retry: {
        retries: 30,
        initialRetryTime: 500,
        maxRetryTime: 3000
      }
    });

    this.admin = this.kafka.admin();

    console.log('Connecting to Apache Kafka:', brokers.join(', '));

    await this.admin.connect();

    await this.ensureTopic();

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: false
    });

    await this.producer.connect();

    this.topicReady = true;
    this.mode = 'real-apache-kafka';

    console.log('==============================================');
    console.log('Connected to REAL Apache Kafka');
    console.log(`Topic: ${TOPIC}`);
    console.log(`Partitions: ${PARTITIONS}`);
    console.log('==============================================');

    for (const group of this.consumerGroups.values()) {
      this.startConsumer(group).catch(err => {
        console.error(
          `Consumer ${group.groupId} failed:`,
          err.message
        );
      });
    }

    await this.refreshSnapshot();
  }

  async ensureTopic() {
    const topics = await this.admin.listTopics();

    if (!topics.includes(TOPIC)) {
      await this.admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic: TOPIC,
            numPartitions: PARTITIONS,
            replicationFactor: 1
          }
        ]
      });

      console.log(
        `Created Kafka topic "${TOPIC}" with ${PARTITIONS} partitions`
      );
    }
  }

  async waitUntilReady() {
    await this.ready;
  }

  async produce(topic, key, value, headers = {}) {
    await this.waitUntilReady();

    let payload;

    if (Buffer.isBuffer(value)) {
      payload = value;
    } else if (value && Buffer.isBuffer(value.data)) {
      payload = value.data;
    } else {
      payload = Buffer.from(JSON.stringify(value));
    }

    const kafkaHeaders = {};

    for (const [headerKey, headerValue] of Object.entries(headers)) {
      kafkaHeaders[headerKey] = Buffer.from(String(headerValue));
    }

    const result = await this.producer.send({
      topic,
      messages: [
        {
          key: key ? String(key) : null,
          value: payload,
          headers: kafkaHeaders
        }
      ]
    });

    const record = result[0];

    const produced = {
      topic,
      partition: record.partition,
      offset: Number(record.offset),
      key,
      size: payload.length,
      timestamp: Date.now()
    };

    this.stats.totalProduced++;
    this.stats.totalBytesProduced += payload.length;

    this.recentBytes.push({
      time: Date.now(),
      bytes: payload.length
    });

    this.emit('message:produced', produced);

    return produced;
  }

  decodeMessage(message) {
    const headers = message.headers || {};

    const header = name => {
      if (!headers[name]) {
        return undefined;
      }

      return headers[name].toString();
    };

    const contentType =
      header('content-type') || 'application/octet-stream';

    if (contentType.startsWith('video/')) {
      return {
        streamId: header('stream-id'),
        chunkIndex: Number(header('chunk-index')),
        totalChunks: Number(header('total-chunks')),
        startByte: Number(header('start-byte')),
        endByte: Number(header('end-byte')),
        chunkSize: Number(header('chunk-size')),
        totalBytes: Number(header('total-bytes')),
        mimeType: contentType,
        checksum: header('checksum'),
        sourceFilename: header('source-filename'),
        base64Data: message.value.toString('base64')
      };
    }

    try {
      return JSON.parse(message.value.toString());
    } catch {
      return {
        rawBase64: message.value.toString('base64')
      };
    }
  }

  registerConsumerGroup(groupId, topicName, onMessage) {
    const group = {
      groupId,
      topicName,
      onMessage,
      consumer: null,
      isPaused: false,
      messagesProcessed: 0,
      bytesProcessed: 0,
      started: false
    };

    this.consumerGroups.set(groupId, group);

    this.emit('consumer:registered', {
      groupId,
      topicName
    });

    if (this.topicReady) {
      this.startConsumer(group).catch(err => {
        console.error(
          `Consumer ${groupId} failed:`,
          err.message
        );
      });
    }

    return group;
  }

  async startConsumer(group) {
    if (group.started) {
      return;
    }

    group.started = true;

    const consumer = this.kafka.consumer({
      groupId: group.groupId,
      allowAutoTopicCreation: false,
      sessionTimeout: 30000,
      heartbeatInterval: 3000
    });

    group.consumer = consumer;

    await consumer.connect();

    await consumer.subscribe({
      topic: group.topicName,
      fromBeginning: true
    });

    console.log(
      `Consumer connected: ${group.groupId}`
    );

    consumer.run({
      autoCommit: false,

      eachMessage: async ({
        topic,
        partition,
        message,
        heartbeat
      }) => {
        try {
          const value = this.decodeMessage(message);

          const metadata = {
            groupId: group.groupId,
            topic,
            partition,
            offset: Number(message.offset),
            committedOffset: Number(message.offset) + 1,
            timestamp: message.timestamp,
            isCatchUp: false
          };

          await group.onMessage(
            {
              key: message.key
                ? message.key.toString()
                : null,
              value,
              rawValue: message.value
            },
            metadata
          );

          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: String(
                Number(message.offset) + 1
              )
            }
          ]);

          group.messagesProcessed++;
          group.bytesProcessed += message.value.length;

          await heartbeat();
        } catch (err) {
          console.error(
            `Error processing message in ${group.groupId}:`,
            err
          );

          throw err;
        }
      }
    }).catch(err => {
      console.error(
        `Kafka consumer stopped for ${group.groupId}:`,
        err
      );

      group.started = false;
    });
  }

  async pauseConsumerGroup(groupId) {
    const group = this.consumerGroups.get(groupId);

    if (!group || !group.consumer) {
      return false;
    }

    group.consumer.pause([
      {
        topic: group.topicName
      }
    ]);

    group.isPaused = true;

    this.emit('consumer:paused', {
      groupId
    });

    await this.refreshSnapshot();

    return true;
  }

  async resumeConsumerGroup(groupId) {
    const group = this.consumerGroups.get(groupId);

    if (!group || !group.consumer) {
      return false;
    }

    group.consumer.resume([
      {
        topic: group.topicName
      }
    ]);

    group.isPaused = false;

    this.emit('consumer:resumed', {
      groupId
    });

    await this.refreshSnapshot();

    return true;
  }

  async resetAll() {
    await this.waitUntilReady();

    for (const group of this.consumerGroups.values()) {
      if (group.consumer) {
        try {
          await group.consumer.stop();
        } catch { }

        try {
          await group.consumer.disconnect();
        } catch { }
      }

      group.consumer = null;
      group.started = false;
      group.isPaused = false;
      group.messagesProcessed = 0;
      group.bytesProcessed = 0;
    }

    try {
      await this.admin.deleteGroups(CONSUMER_GROUPS);
    } catch { }

    try {
      await this.admin.deleteTopics({
        topics: [TOPIC]
      });
    } catch { }

    await new Promise(resolve =>
      setTimeout(resolve, 1000)
    );

    await this.admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic: TOPIC,
          numPartitions: PARTITIONS,
          replicationFactor: 1
        }
      ]
    });

    this.stats = {
      totalProduced: 0,
      totalBytesProduced: 0,
      produceRateBps: 0,
      messagesPerSec: 0,
      startTime: Date.now()
    };

    this.recentBytes = [];

    for (const group of this.consumerGroups.values()) {
      await this.startConsumer(group);
    }

    this.emit('system:reset');

    await this.refreshSnapshot();
  }

  calculateThroughput() {
    const now = Date.now();

    this.recentBytes = this.recentBytes.filter(
      item => now - item.time < 3000
    );

    const totalBytes = this.recentBytes.reduce(
      (sum, item) => sum + item.bytes,
      0
    );

    this.stats.produceRateBps = Math.round(
      totalBytes / 3
    );

    this.stats.messagesPerSec = Math.round(
      this.recentBytes.length / 3
    );
  }

  async refreshSnapshot() {
    if (!this.admin || !this.topicReady) {
      return;
    }

    this.calculateThroughput();

    const topicOffsets =
      await this.admin.fetchTopicOffsets(TOPIC);

    const partitions = topicOffsets.map(partition => ({
      partitionId: Number(partition.partition),
      messageCount: Number(partition.offset),
      latestOffset:
        Number(partition.offset) > 0
          ? Number(partition.offset) - 1
          : -1,
      recentMessages: []
    }));

    const consumers = {};

    for (const group of this.consumerGroups.values()) {
      let committed = [];

      try {
        const result =
          await this.admin.fetchOffsets({
            groupId: group.groupId,
            topics: [TOPIC]
          });

        committed =
          result[0]?.partitions || [];
      } catch { }

      const committedMap = Object.fromEntries(
        committed.map(item => [
          Number(item.partition),
          Number(item.offset)
        ])
      );

      const committedOffsets =
        partitions.map(
          partition =>
            committedMap[partition.partitionId] ?? 0
        );

      const lagPerPartition =
        partitions.map(partition =>
          Math.max(
            0,
            partition.messageCount -
            (committedMap[partition.partitionId] ?? 0)
          )
        );

      const lag = lagPerPartition.reduce(
        (sum, value) => sum + value,
        0
      );

      consumers[group.groupId] = {
        groupId: group.groupId,
        topicName: TOPIC,
        isPaused: group.isPaused,
        committedOffsets,
        messagesProcessed: group.messagesProcessed,
        bytesProcessed: group.bytesProcessed,
        lag,
        lagPerPartition
      };
    }

    this.snapshot = {
      mode: this.mode,
      stats: this.stats,
      topics: {
        [TOPIC]: {
          name: TOPIC,
          numPartitions: PARTITIONS,
          totalMessages: partitions.reduce(
            (sum, partition) =>
              sum + partition.messageCount,
            0
          ),
          partitions
        }
      },
      consumers
    };
  }

  getSnapshot() {
    return this.snapshot;
  }
}

module.exports = new KafkaEngine();