# Queue Cleaner

Queue Cleaner is a cross-platform Electron application that helps debug RabbitMQ dead-letter queues (DLX). It lists queues that are configured with a dead-letter exchange or queue, lets you inspect messages currently in the DLX, and allows you to move selected messages back to their processing queues.

## Prerequisites

- [Node.js](https://nodejs.org/) 18+
- [npm](https://www.npmjs.com/)
- [1Password CLI](https://developer.1password.com/docs/cli) (`op`) with access to the RabbitMQ credentials that should be injected
- RabbitMQ Management Plugin enabled on the target cluster

## Configuration

Credentials and connection details are loaded via the 1Password CLI using [op inject](https://developer.1password.com/docs/cli/reference/commands/inject/).

1. Duplicate `.env.template` and update the `op://` references to match your 1Password vault structure.
2. Run `op inject` to materialize the `.env` file:

   ```bash
   op inject -i .env.template -o .env
   ```

3. Verify that `.env` now contains real values for the environments you plan to target:

   - Test (default): `RABBITMQ_TEST_HOST`, `RABBITMQ_TEST_USERNAME`, `RABBITMQ_TEST_PASSWORD`, and `RABBITMQ_TEST_VHOST`
   - Production (optional): `RABBITMQ_PROD_HOST`, `RABBITMQ_PROD_USERNAME`, `RABBITMQ_PROD_PASSWORD`, and `RABBITMQ_PROD_VHOST`
   - Shared overrides such as `RABBITMQ_AMQP_PORT`, `RABBITMQ_MANAGEMENT_PORT`, `RABBITMQ_AMQP_PROTOCOL`, `RABBITMQ_MANAGEMENT_PROTOCOL`, and `RABBITMQ_REQUEST_TIMEOUT_MS`

> **Note:** `.env` is git-ignored and should not be committed.

## Installation

Install dependencies after injecting credentials:

```bash
npm install
```

## Development

Start the Electron app in development mode:

```bash
npm start
```

The main window lists all queues that have a dead-letter configuration. Selecting a queue shows the bound DLX queue(s) and allows you to load and inspect messages. Use the **Environment** dropdown above the filters to switch between the configured RabbitMQ targets—production is flagged with a red badge so you can immediately see when you are acting on live queues. Use the **Move to processing queue** action to republish a message to the original queue. The app attempts to remove the matching message from the DLX after a successful publish.

## Packaging

Electron Builder is configured for cross-platform builds. Create distributables with:

```bash
npm run dist
```

Artifacts are output to the `release/` directory.

## Troubleshooting

- Ensure the RabbitMQ management endpoint is reachable from the machine running Queue Cleaner.
- The user specified in `.env` must have permissions to read queues, inspect dead-letter exchanges, and publish messages.
- If message removal from the DLX fails (for example, if the queue is modified concurrently), the UI will display an error. You can refresh the message list to confirm the state.
