const path = require('path');
const dotenv = require('dotenv');

const envPath = path.resolve(process.cwd(), '.env');
dotenv.config({ path: envPath, override: false });

const DEFAULTS = {
  host: 'localhost',
  username: 'guest',
  password: 'guest',
  managementProtocol: 'http',
  managementPort: 15672,
  amqpProtocol: 'amqp',
  amqpPort: 5672,
  vhost: '/',
  requestTimeoutMs: 10000
};

const REQUIRED_ENV_KEYS = ['HOST', 'USERNAME', 'PASSWORD'];

const ENVIRONMENT_DEFINITIONS = [
  {
    key: 'test',
    prefix: 'RABBITMQ_TEST_',
    label: 'Test',
    isProd: false
  },
  {
    key: 'prod',
    prefix: 'RABBITMQ_PROD_',
    label: 'Production',
    isProd: true
  }
];

const cleanVhost = (vhost) => {
  if (!vhost) return '/';
  if (vhost.startsWith('/')) return vhost;
  return `/${vhost}`;
};

const pickEnvValue = (prefix, key) => {
  const value = process.env[`${prefix}${key}`];
  if (value === undefined || value === null || value === '') {
    return undefined;
  }
  return value;
};

const assembleConfig = (raw) => {
  const config = { ...raw };
  config.vhost = cleanVhost(config.vhost || DEFAULTS.vhost);
  config.managementProtocol = config.managementProtocol || DEFAULTS.managementProtocol;
  config.managementPort = Number(config.managementPort ?? DEFAULTS.managementPort);
  config.amqpProtocol = config.amqpProtocol || DEFAULTS.amqpProtocol;
  config.amqpPort = Number(config.amqpPort ?? DEFAULTS.amqpPort);
  config.requestTimeoutMs = Number(config.requestTimeoutMs ?? DEFAULTS.requestTimeoutMs);

  config.managementBaseUrl = `${config.managementProtocol}://${config.host}:${config.managementPort}/api`;
  const encodedVhost = encodeURIComponent(config.vhost === '/' ? '/' : config.vhost.slice(1));
  config.amqpUrl = `${config.amqpProtocol}://${encodeURIComponent(config.username)}:${encodeURIComponent(
    config.password
  )}@${config.host}:${config.amqpPort}/${config.vhost === '/' ? '' : encodedVhost}`;

  return config;
};

const buildConfig = (prefix, fallback = {}) => {
  const raw = {
    host: pickEnvValue(prefix, 'HOST') ?? fallback.host ?? DEFAULTS.host,
    username: pickEnvValue(prefix, 'USERNAME') ?? fallback.username ?? DEFAULTS.username,
    password: pickEnvValue(prefix, 'PASSWORD') ?? fallback.password ?? DEFAULTS.password,
    managementProtocol:
      pickEnvValue(prefix, 'MANAGEMENT_PROTOCOL') ?? fallback.managementProtocol ?? DEFAULTS.managementProtocol,
    managementPort:
      pickEnvValue(prefix, 'MANAGEMENT_PORT') ?? fallback.managementPort ?? DEFAULTS.managementPort,
    amqpProtocol: pickEnvValue(prefix, 'AMQP_PROTOCOL') ?? fallback.amqpProtocol ?? DEFAULTS.amqpProtocol,
    amqpPort: pickEnvValue(prefix, 'AMQP_PORT') ?? fallback.amqpPort ?? DEFAULTS.amqpPort,
    vhost: pickEnvValue(prefix, 'VHOST') ?? fallback.vhost ?? DEFAULTS.vhost,
    requestTimeoutMs:
      pickEnvValue(prefix, 'REQUEST_TIMEOUT_MS') ?? fallback.requestTimeoutMs ?? DEFAULTS.requestTimeoutMs
  };

  return assembleConfig(raw);
};

const baseFallback = buildConfig('RABBITMQ_');

const environmentConfigs = {};
const environmentOrder = [];

const registerEnvironment = (definition, config) => {
  if (!config || environmentConfigs[definition.key]) {
    return;
  }

  environmentConfigs[definition.key] = {
    ...config,
    key: definition.key,
    label: pickEnvValue(definition.prefix, 'LABEL') || definition.label,
    isProd: Boolean(definition.isProd)
  };
  environmentOrder.push(definition.key);
};

const hasRequiredCredentials = (definition) =>
  REQUIRED_ENV_KEYS.every((key) => pickEnvValue(definition.prefix, key) !== undefined);

for (const definition of ENVIRONMENT_DEFINITIONS) {
  if (hasRequiredCredentials(definition)) {
    registerEnvironment(definition, buildConfig(definition.prefix, baseFallback));
  }
}

const testDefinition = ENVIRONMENT_DEFINITIONS.find((def) => def.key === 'test');
if (testDefinition && !environmentConfigs[testDefinition.key] && !environmentOrder.length) {
  registerEnvironment(testDefinition, baseFallback);
}

if (!environmentOrder.length) {
  registerEnvironment(
    {
      key: 'default',
      prefix: 'RABBITMQ_',
      label: 'Default',
      isProd: false
    },
    baseFallback
  );
}

const getEnvironmentMetadata = () =>
  environmentOrder.map((key) => {
    const { label, isProd } = environmentConfigs[key];
    return { key, label, isProd };
  });

const getEnvironmentConfig = (key) => environmentConfigs[key] || null;

const defaultEnvironment = environmentConfigs.test ? 'test' : environmentOrder[0];

module.exports = {
  defaultEnvironment,
  getEnvironmentConfig,
  getEnvironmentMetadata
};
