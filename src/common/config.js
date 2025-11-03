const path = require('path');
const dotenv = require('dotenv');

const envPath = path.resolve(process.cwd(), '.env');
dotenv.config({ path: envPath, override: false });

const required = ['RABBITMQ_HOST', 'RABBITMQ_USERNAME', 'RABBITMQ_PASSWORD'];
const missing = required.filter((key) => !process.env[key]);

if (missing.length) {
  console.warn(
    `Missing RabbitMQ configuration values: ${missing.join(', ')}. ` +
      'Use op inject with .env.template to populate them before running the app.'
  );
}

const cleanVhost = (vhost) => {
  if (!vhost) return '/';
  if (vhost.startsWith('/')) return vhost;
  return `/${vhost}`;
};

const config = {
  host: process.env.RABBITMQ_HOST || 'localhost',
  username: process.env.RABBITMQ_USERNAME || 'guest',
  password: process.env.RABBITMQ_PASSWORD || 'guest',
  managementProtocol: process.env.RABBITMQ_MANAGEMENT_PROTOCOL || 'http',
  managementPort: Number(process.env.RABBITMQ_MANAGEMENT_PORT || 15672),
  amqpProtocol: process.env.RABBITMQ_AMQP_PROTOCOL || 'amqp',
  amqpPort: Number(process.env.RABBITMQ_AMQP_PORT || 5672),
  vhost: cleanVhost(process.env.RABBITMQ_VHOST || '/'),
  requestTimeoutMs: Number(process.env.RABBITMQ_REQUEST_TIMEOUT_MS || 10000)
};

config.managementBaseUrl = `${config.managementProtocol}://${config.host}:${config.managementPort}/api`;
const encodedVhost = encodeURIComponent(config.vhost === '/' ? '/' : config.vhost.slice(1));
config.amqpUrl = `${config.amqpProtocol}://${encodeURIComponent(config.username)}:${encodeURIComponent(
  config.password
)}@${config.host}:${config.amqpPort}/${config.vhost === '/' ? '' : encodedVhost}`;

module.exports = config;
