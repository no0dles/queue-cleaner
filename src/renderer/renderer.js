const state = {
  queues: [],
  selectedQueue: null,
  selectedDlxQueue: null,
  messages: []
};

const queueListEl = document.getElementById('queue-list');
const queueDetailsEl = document.getElementById('queue-details');
const emptyStateEl = document.getElementById('empty-state');
const queueNameEl = document.getElementById('queue-name');
const queueVhostEl = document.getElementById('queue-vhost');
const queueDlxEl = document.getElementById('queue-dlx');
const dlxSelectEl = document.getElementById('dlx-select');
const messageCountEl = document.getElementById('message-count');
const loadMessagesBtn = document.getElementById('load-messages');
const messagesContainerEl = document.getElementById('messages-container');
const refreshQueuesBtn = document.getElementById('refresh-queues');
const statusEl = document.getElementById('status');

async function initialize() {
  attachEvents();
  await loadQueues();
}

function attachEvents() {
  refreshQueuesBtn.addEventListener('click', () => loadQueues());
  loadMessagesBtn.addEventListener('click', () => {
    if (state.selectedQueue && state.selectedDlxQueue) {
      loadMessages();
    }
  });

  dlxSelectEl.addEventListener('change', () => {
    const dlxName = dlxSelectEl.value;
    if (!dlxName) return;
    const selected = state.selectedQueue.dlxQueues.find((q) => q.name === dlxName);
    if (selected) {
      state.selectedDlxQueue = selected;
      loadMessages();
      renderQueueSummary();
    }
  });
}

async function loadQueues() {
  setStatus('Loading queues...');
  toggleQueueDetails(false);
  queueListEl.innerHTML = '<p>Loading...</p>';

  try {
    const queues = await window.api.listQueues();
    state.queues = queues;
    renderQueueList();
    setStatus(`Loaded ${queues.length} queues with DLX.`);
  } catch (error) {
    console.error(error);
    queueListEl.innerHTML = '<p class="error">Failed to load queues.</p>';
    setStatus(`Error: ${error.message}`);
  }
}

function renderQueueList() {
  queueListEl.innerHTML = '';

  if (!state.queues.length) {
    queueListEl.innerHTML = '<p>No queues configured with a dead-letter exchange.</p>';
    return;
  }

  state.queues.forEach((queue) => {
    const item = document.createElement('div');
    item.className = 'queue-item';
    item.dataset.queue = queue.name;
    item.innerHTML = `
      <h3>${queue.name}</h3>
      <p>VHost: ${queue.vhost}</p>
      <p>DLX: ${queue.deadLetterExchange || queue.dlxQueues.map((q) => q.name).join(', ')}</p>
      <p>Ready messages: ${queue.messagesReady}</p>
    `;

    item.addEventListener('click', () => {
      document.querySelectorAll('.queue-item').forEach((el) => el.classList.remove('active'));
      item.classList.add('active');
      selectQueue(queue);
    });

    queueListEl.appendChild(item);
  });
}

function selectQueue(queue) {
  state.selectedQueue = queue;
  state.selectedDlxQueue = queue.dlxQueues[0] || null;
  state.messages = [];
  renderQueueSummary();
  renderMessages();
  toggleQueueDetails(true);
}

function renderQueueSummary() {
  if (!state.selectedQueue) return;
  const { selectedQueue, selectedDlxQueue } = state;

  queueNameEl.textContent = selectedQueue.name;
  queueVhostEl.textContent = `VHost: ${selectedQueue.vhost}`;
  queueDlxEl.textContent = selectedQueue.deadLetterExchange
    ? `DLX Exchange: ${selectedQueue.deadLetterExchange}`
    : 'DLX Queue mapping';

  dlxSelectEl.innerHTML = '';
  selectedQueue.dlxQueues.forEach((dlx) => {
    const option = document.createElement('option');
    option.value = dlx.name;
    option.textContent = dlx.routingKey ? `${dlx.name} (rk: ${dlx.routingKey})` : dlx.name;
    if (selectedDlxQueue && selectedDlxQueue.name === dlx.name) {
      option.selected = true;
    }
    dlxSelectEl.appendChild(option);
  });
}

async function loadMessages() {
  if (!state.selectedQueue || !state.selectedDlxQueue) return;
  const count = Number(messageCountEl.value) || 50;

  setStatus(`Loading messages from ${state.selectedDlxQueue.name}...`);
  messagesContainerEl.innerHTML = '<p>Loading messages...</p>';

  try {
    const messages = await window.api.getMessages({
      vhost: state.selectedQueue.vhost,
      queue: state.selectedDlxQueue.name,
      count
    });
    state.messages = messages;
    renderMessages();
    setStatus(`Loaded ${messages.length} messages from ${state.selectedDlxQueue.name}.`);
  } catch (error) {
    console.error(error);
    messagesContainerEl.innerHTML = '<p class="error">Failed to load messages.</p>';
    setStatus(`Error: ${error.message}`);
  }
}

function renderMessages() {
  if (!state.messages.length) {
    messagesContainerEl.innerHTML = '<p>No messages found in the DLX queue.</p>';
    return;
  }

  messagesContainerEl.innerHTML = '';

  state.messages.forEach((message, index) => {
    const card = document.createElement('article');
    card.className = 'message-card';

    const header = document.createElement('header');
    const title = document.createElement('h3');
    title.textContent = `Message #${index + 1}`;
    header.appendChild(title);

    const meta = document.createElement('div');
    meta.className = 'message-meta';
    const metaParts = [];
    if (message.properties?.messageId) {
      metaParts.push(`ID: ${message.properties.messageId}`);
    }
    if (message.properties?.timestamp) {
      metaParts.push(`Timestamp: ${formatTimestamp(message.properties.timestamp)}`);
    }
    if (message.routingKey) {
      metaParts.push(`Routing Key: ${message.routingKey}`);
    }
    meta.textContent = metaParts.join(' | ');

    const payload = document.createElement('pre');
    payload.textContent = safeStringify(message.payloadText);

    const properties = document.createElement('pre');
    properties.textContent = JSON.stringify(message.properties, null, 2);

    const headers = document.createElement('pre');
    headers.textContent = JSON.stringify(message.headers || {}, null, 2);

    const actions = document.createElement('div');
    actions.className = 'message-actions';

    const moveBtn = document.createElement('button');
    moveBtn.className = 'move';
    moveBtn.textContent = 'Move to processing queue';
    moveBtn.addEventListener('click', async () => {
      await handleMoveMessage(message, moveBtn);
    });

    const copyBtn = document.createElement('button');
    copyBtn.className = 'copy';
    copyBtn.textContent = 'Copy payload';
    copyBtn.addEventListener('click', () => {
      navigator.clipboard.writeText(message.payloadText || '');
      setStatus('Payload copied to clipboard.');
    });

    actions.appendChild(moveBtn);
    actions.appendChild(copyBtn);

    card.appendChild(header);
    card.appendChild(meta);
    card.appendChild(document.createElement('h4')).textContent = 'Payload';
    card.appendChild(payload);
    card.appendChild(document.createElement('h4')).textContent = 'Properties';
    card.appendChild(properties);
    card.appendChild(document.createElement('h4')).textContent = 'Headers';
    card.appendChild(headers);
    card.appendChild(actions);

    messagesContainerEl.appendChild(card);
  });
}

async function handleMoveMessage(message, button) {
  if (!state.selectedQueue || !state.selectedDlxQueue) return;

  const previousText = button.textContent;
  button.disabled = true;
  button.textContent = 'Moving...';
  setStatus('Moving message back to processing queue...');

  try {
    await window.api.moveMessage({
      vhost: state.selectedQueue.vhost,
      dlxQueue: state.selectedDlxQueue.name,
      targetQueue: state.selectedQueue.name,
      targetRoutingKey: state.selectedDlxQueue.routingKey,
      message
    });
    setStatus('Message moved successfully. Refreshing...');
    await loadMessages();
  } catch (error) {
    console.error(error);
    setStatus(`Error moving message: ${error.message}`);
  } finally {
    button.disabled = false;
    button.textContent = previousText;
  }
}

function toggleQueueDetails(show) {
  if (show) {
    queueDetailsEl.classList.remove('hidden');
    emptyStateEl.style.display = 'none';
  } else {
    queueDetailsEl.classList.add('hidden');
    emptyStateEl.style.display = 'flex';
  }
}

function setStatus(text) {
  statusEl.textContent = text;
}

function formatTimestamp(timestamp) {
  if (!timestamp) return '';
  const date = new Date(timestamp * 1000);
  return date.toLocaleString();
}

function safeStringify(value) {
  if (!value) return '';
  try {
    const parsed = JSON.parse(value);
    return JSON.stringify(parsed, null, 2);
  } catch (error) {
    return value;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  initialize().catch((error) => {
    console.error(error);
    setStatus(`Initialization error: ${error.message}`);
  });
});
