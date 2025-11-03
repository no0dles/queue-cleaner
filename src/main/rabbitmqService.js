const amqp = require('amqplib');
const config = require('../common/config');

class RabbitMQService {
  constructor() {
    this.config = config;
    this.connection = null;
    this.channel = null;
    this.bindingCache = new Map();
  }

  async ensureChannel() {
    if (this.channel) {
      return this.channel;
    }

    if (!this.connection) {
      this.connection = await amqp.connect(this.config.amqpUrl);
      this.connection.on('close', () => {
        this.connection = null;
        this.channel = null;
      });
      this.connection.on('error', () => {
        this.connection = null;
        this.channel = null;
      });
    }

    this.channel = await this.connection.createChannel();
    return this.channel;
  }

  async close() {
    if (this.channel) {
      try {
        await this.channel.close();
      } catch (error) {
        // ignore
      }
      this.channel = null;
    }
    if (this.connection) {
      try {
        await this.connection.close();
      } catch (error) {
        // ignore
      }
      this.connection = null;
    }
  }

  async fetchJSON(path, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.config.requestTimeoutMs);

    const response = await fetch(`${this.config.managementBaseUrl}${path}`, {
      ...options,
      headers: {
        Authorization: `Basic ${Buffer.from(`${this.config.username}:${this.config.password}`).toString('base64')}`,
        'Content-Type': 'application/json',
        ...(options.headers || {})
      },
      signal: controller.signal
    });

    clearTimeout(timeout);

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`RabbitMQ management request failed (${response.status}): ${text}`);
    }

    return response.json();
  }

  async listQueuesWithDLX() {
    const queues = await this.fetchJSON('/queues');
    const queueIndex = new Map();
    const results = [];

    for (const queue of queues) {
      queueIndex.set(`${queue.vhost}::${queue.name}`, queue);
    }

    for (const queue of queues) {
      const args = queue.arguments || {};
      const dlxQueue = args['x-dead-letter-queue'];
      const dlxExchange = args['x-dead-letter-exchange'];

      if (!dlxQueue && !dlxExchange) {
        continue;
      }

      const entry = {
        name: queue.name,
        vhost: queue.vhost,
        messages: queue.messages,
        messagesReady: 0,
        deadLetterExchange: dlxExchange || null,
        deadLetterRoutingKey: args['x-dead-letter-routing-key'] || null,
        dlxQueues: []
      };

      if (dlxQueue) {
        const dlxInfo = queueIndex.get(`${queue.vhost}::${dlxQueue}`);
        entry.dlxQueues.push({
          name: dlxQueue,
          routingKey: args['x-dead-letter-routing-key'] || dlxQueue,
          messagesReady: dlxInfo ? dlxInfo.messages_ready : 0
        });
      }

      if (dlxExchange) {
        const cacheKey = `${queue.vhost}::${dlxExchange}`;
        let bindings = this.bindingCache.get(cacheKey);
        if (!bindings) {
          bindings = await this.fetchJSON(
            `/exchanges/${encodeURIComponent(queue.vhost)}/${encodeURIComponent(dlxExchange)}/bindings/source`
          );
          this.bindingCache.set(cacheKey, bindings);
        }

        const queueBindings = bindings.filter((binding) => binding.destination_type === 'queue');
        for (const binding of queueBindings) {
          const bindingInfo = queueIndex.get(`${queue.vhost}::${binding.destination}`);
          entry.dlxQueues.push({
            name: binding.destination,
            routingKey: binding.routing_key,
            messagesReady: bindingInfo ? bindingInfo.messages_ready : 0
          });
        }
      }

      // Deduplicate DLX queues by name
      const seen = new Map();
      entry.dlxQueues = entry.dlxQueues.filter((q) => {
        if (seen.has(q.name)) {
          return false;
        }
        seen.set(q.name, true);
        return true;
      });

      if (entry.dlxQueues.length) {
        entry.messagesReady = entry.dlxQueues.reduce((sum, dlx) => sum + (dlx.messagesReady || 0), 0);
        results.push(entry);
      }
    }

    return results;
  }

  async getMessages({ vhost, queue, count = 50 }) {
    const body = {
      count,
      ackmode: 'ack_requeue_true',
      encoding: 'base64',
      truncate: 0
    };

    const messages = await this.fetchJSON(
      `/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(queue)}/get`,
      {
        method: 'POST',
        body: JSON.stringify(body)
      }
    );

    return messages.map((message) => this.transformManagementMessage(message));
  }

  transformManagementMessage(message) {
    const { payload, payload_encoding: payloadEncoding, properties = {}, headers = {}, ...rest } = message;
    const { routing_key: routingKey, exchange, ...restFields } = rest;
    const payloadBase64 = payloadEncoding === 'base64' ? payload : Buffer.from(payload, 'utf8').toString('base64');

    return {
      ...restFields,
      exchange,
      routingKey,
      payloadEncoding,
      payloadBase64,
      payloadText: this.decodePayload(payload, payloadEncoding),
      properties: this.normalizeProperties(properties),
      headers
    };
  }

  decodePayload(payload, encoding) {
    if (encoding === 'base64') {
      return Buffer.from(payload, 'base64').toString('utf8');
    }
    return payload;
  }

  normalizeProperties(properties) {
    const normalized = {};
    for (const [key, value] of Object.entries(properties || {})) {
      const camelKey = key.replace(/_([a-z])/g, (_, chr) => chr.toUpperCase());
      const finalKey = camelKey.replace(/^([a-z])/, (chr) => chr.toLowerCase());
      normalized[finalKey] = value;
    }
    return normalized;
  }

  async moveMessage({ vhost, dlxQueue, targetQueue, targetRoutingKey, message }) {
    const channel = await this.ensureChannel();
    await channel.checkQueue(dlxQueue);
    await channel.checkQueue(targetQueue);

    const contentBuffer = Buffer.from(message.payloadBase64, 'base64');
    const publishOptions = this.buildPublishOptions(message);
    const routingKey = targetRoutingKey || message.routingKey || targetQueue;

    const published = channel.publish('', routingKey, contentBuffer, publishOptions);
    if (!published) {
      await this.waitForDrain(channel);
    }

    const removed = await this.removeMessageFromQueue(channel, dlxQueue, message);

    return { published: true, removed };
  }

  buildPublishOptions(message) {
    const properties = message.properties || {};
    const options = {
      headers: this.sanitizeHeaders(message.headers || {})
    };

    const map = {
      contentType: 'contentType',
      contentEncoding: 'contentEncoding',
      correlationId: 'correlationId',
      messageId: 'messageId',
      type: 'type',
      appId: 'appId',
      replyTo: 'replyTo',
      expiration: 'expiration'
    };

    for (const [key, target] of Object.entries(map)) {
      if (properties[key] !== undefined) {
        options[target] = properties[key];
      }
    }

    if (properties.priority !== undefined) {
      options.priority = properties.priority;
    }

    if (properties.timestamp !== undefined) {
      options.timestamp = properties.timestamp;
    }

    if (properties.deliveryMode !== undefined) {
      options.persistent = properties.deliveryMode === 2;
    }

    return options;
  }

  sanitizeHeaders(headers) {
    const sanitized = {};
    for (const [key, value] of Object.entries(headers || {})) {
      if (!/^x-/i.test(key)) {
        sanitized[key] = value;
      }
    }
    return sanitized;
  }

  async removeMessageFromQueue(channel, queueName, targetMessage) {
    const maxScans = 200;
    for (let i = 0; i < maxScans; i += 1) {
      const msg = await channel.get(queueName, { noAck: false });
      if (!msg) {
        return false;
      }

      if (this.matchesMessage(msg, targetMessage)) {
        channel.ack(msg);
        return true;
      }

      channel.nack(msg, false, true);
    }

    throw new Error('Unable to locate message in DLX queue to remove after republishing.');
  }

  matchesMessage(amqpMessage, targetMessage) {
    const targetContent = targetMessage.payloadBase64;
    const amqpContent = amqpMessage.content.toString('base64');

    if (targetContent !== amqpContent) {
      return false;
    }

    const targetId = targetMessage.properties?.messageId;
    const amqpId = amqpMessage.properties.messageId;
    if (targetId && amqpId && targetId !== amqpId) {
      return false;
    }

    return true;
  }

  buildPublishOptionsFromAmqpMessage(message) {
    const options = {
      headers: this.sanitizeHeaders(message.properties?.headers || {})
    };

    const copyFields = [
      'contentType',
      'contentEncoding',
      'correlationId',
      'messageId',
      'type',
      'appId',
      'replyTo',
      'expiration'
    ];

    for (const key of copyFields) {
      if (message.properties?.[key] !== undefined) {
        options[key] = message.properties[key];
      }
    }

    if (message.properties?.priority !== undefined) {
      options.priority = message.properties.priority;
    }

    if (message.properties?.timestamp !== undefined) {
      options.timestamp = message.properties.timestamp;
    }

    if (message.properties?.deliveryMode !== undefined) {
      options.persistent = message.properties.deliveryMode === 2;
    }

    return options;
  }

  async waitForDrain(channel) {
    await new Promise((resolve) => channel.once('drain', resolve));
  }

  async moveAllMessages({ vhost, dlxQueue, targetQueue, targetRoutingKey }) {
    const channel = await this.ensureChannel();
    await channel.checkQueue(dlxQueue);
    await channel.checkQueue(targetQueue);

    let moved = 0;

    while (true) {
      const message = await channel.get(dlxQueue, { noAck: false });
      if (!message) {
        break;
      }

      try {
        const routingKey = targetRoutingKey || message.fields.routingKey || targetQueue;
        const options = this.buildPublishOptionsFromAmqpMessage(message);
        const published = channel.publish('', routingKey, message.content, options);
        if (!published) {
          await this.waitForDrain(channel);
        }
        channel.ack(message);
        moved += 1;
      } catch (error) {
        channel.nack(message, false, true);
        throw error;
      }
    }

    return { moved };
  }
}

module.exports = RabbitMQService;
