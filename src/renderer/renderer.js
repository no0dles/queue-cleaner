const DEFAULT_VHOST_FILTER = '__all__';

const state = {
  queues: [],
  selectedQueue: null,
  selectedDlxQueue: null,
  messages: [],
  filterVhost: DEFAULT_VHOST_FILTER,
  searchTerm: ''
};

const queueListEl = document.getElementById('queue-list');
const queueDetailsEl = document.getElementById('queue-details');
const emptyStateEl = document.getElementById('empty-state');
const queueNameEl = document.getElementById('queue-name');
const queueVhostEl = document.getElementById('queue-vhost');
const queueDlxEl = document.getElementById('queue-dlx');
const queueReadyEl = document.getElementById('queue-ready');
const dlxSelectEl = document.getElementById('dlx-select');
const messageCountEl = document.getElementById('message-count');
const loadMessagesBtn = document.getElementById('load-messages');
const messagesContainerEl = document.getElementById('messages-container');
const refreshQueuesBtn = document.getElementById('refresh-queues');
const statusEl = document.getElementById('status');
const vhostFilterEl = document.getElementById('vhost-filter');
const queueSearchEl = document.getElementById('queue-search');
const moveAllBtn = document.getElementById('move-all');

async function initialize() {
  attachEvents();
  await loadQueues();
}

function attachEvents() {
  refreshQueuesBtn.addEventListener('click', () => loadQueues({ preserveSelection: true }));

  loadMessagesBtn.addEventListener('click', () => {
    if (state.selectedQueue && state.selectedDlxQueue) {
      loadMessages();
    }
  });

  dlxSelectEl.addEventListener('change', () => {
    if (!state.selectedQueue) return;
    const dlxName = dlxSelectEl.value;
    if (!dlxName) return;
    const selected = state.selectedQueue.dlxQueues.find((q) => q.name === dlxName);
    if (selected) {
      state.selectedDlxQueue = selected;
      loadMessages();
      renderQueueSummary();
    }
  });

  vhostFilterEl.addEventListener('change', () => {
    state.filterVhost = vhostFilterEl.value || DEFAULT_VHOST_FILTER;
    renderQueueList();
    handleFilteredSelection();
  });

  queueSearchEl.addEventListener('input', () => {
    state.searchTerm = queueSearchEl.value.trim().toLowerCase();
    renderQueueList();
    handleFilteredSelection();
  });

  moveAllBtn.addEventListener('click', async () => {
    if (!state.selectedQueue || !state.selectedDlxQueue || moveAllBtn.disabled) {
      return;
    }
    await handleMoveAllMessages(moveAllBtn);
  });
}

async function loadQueues(options = {}) {
  const { preserveSelection = false, silent = false } = options;
  const previousSelection = preserveSelection && state.selectedQueue
    ? {
        name: state.selectedQueue.name,
        vhost: state.selectedQueue.vhost,
        dlx: state.selectedDlxQueue ? state.selectedDlxQueue.name : null
      }
    : null;

  if (!silent) {
    setStatus('Loading queues...');
    queueListEl.innerHTML = '<p>Loading...</p>';
    if (!preserveSelection) {
      toggleQueueDetails(false);
    }
  }

  try {
    const queues = await window.api.listQueues();
    state.queues = queues;
    restoreSelection(previousSelection);
    updateVhostFilterOptions();
    renderQueueList();
    renderQueueSummary();
    renderMessages();
    setStatus(`Loaded ${queues.length} queues with DLX.`);
  } catch (error) {
    console.error(error);
    queueListEl.innerHTML = '<p class="error">Failed to load queues.</p>';
    setStatus(`Error: ${error.message}`);
  }
}

function restoreSelection(previousSelection) {
  if (!previousSelection) {
    return;
  }

  const match = state.queues.find(
    (queue) => queue.name === previousSelection.name && queue.vhost === previousSelection.vhost
  );

  if (match) {
    state.selectedQueue = match;
    state.selectedDlxQueue = previousSelection.dlx
      ? match.dlxQueues.find((dlx) => dlx.name === previousSelection.dlx) || match.dlxQueues[0] || null
      : match.dlxQueues[0] || null;
    toggleQueueDetails(true);
  } else {
    state.selectedQueue = null;
    state.selectedDlxQueue = null;
    state.messages = [];
    toggleQueueDetails(false);
  }
}

function updateVhostFilterOptions() {
  const vhosts = Array.from(new Set(state.queues.map((queue) => queue.vhost))).sort();
  const previousValue = state.filterVhost;

  vhostFilterEl.innerHTML = '';

  const allOption = document.createElement('option');
  allOption.value = DEFAULT_VHOST_FILTER;
  allOption.textContent = 'All virtual hosts';
  vhostFilterEl.appendChild(allOption);

  vhosts.forEach((vhost) => {
    const option = document.createElement('option');
    option.value = vhost;
    option.textContent = vhost;
    vhostFilterEl.appendChild(option);
  });

  if (previousValue !== DEFAULT_VHOST_FILTER && !vhosts.includes(previousValue)) {
    state.filterVhost = DEFAULT_VHOST_FILTER;
  }

  vhostFilterEl.value = state.filterVhost;
}

function getFilteredQueues() {
  const term = state.searchTerm;

  return state.queues.filter((queue) => {
    const matchesVhost =
      state.filterVhost === DEFAULT_VHOST_FILTER || queue.vhost === state.filterVhost;
    const matchesSearch = !term || queue.name.toLowerCase().includes(term);
    return matchesVhost && matchesSearch;
  });
}

function renderQueueList() {
  queueListEl.innerHTML = '';

  if (!state.queues.length) {
    queueListEl.innerHTML = '<p>No queues configured with a dead-letter exchange.</p>';
    return;
  }

  const filtered = getFilteredQueues();

  if (!filtered.length) {
    queueListEl.innerHTML = '<p>No queues match the current filters.</p>';
    return;
  }

  filtered.forEach((queue) => {
    const item = document.createElement('div');
    item.className = 'queue-item';
    item.dataset.queue = queue.name;
    if (state.selectedQueue && queue.name === state.selectedQueue.name && queue.vhost === state.selectedQueue.vhost) {
      item.classList.add('active');
    }

    const dlxLabel = queue.deadLetterExchange
      ? queue.deadLetterExchange
      : queue.dlxQueues.map((q) => `${q.name} (${q.messagesReady} ready)`).join(', ');

    item.innerHTML = `
      <h3>${queue.name}</h3>
      <p>VHost: ${queue.vhost}</p>
      <p>DLX: ${dlxLabel}</p>
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
  renderQueueList();
  renderQueueSummary();
  renderMessages();
  toggleQueueDetails(true);
}

function renderQueueSummary() {
  if (!state.selectedQueue) {
    queueNameEl.textContent = '';
    queueVhostEl.textContent = '';
    queueDlxEl.textContent = '';
    queueReadyEl.textContent = '';
    dlxSelectEl.innerHTML = '';
    updateBulkActions();
    return;
  }

  const { selectedQueue, selectedDlxQueue } = state;

  queueNameEl.textContent = selectedQueue.name;
  queueVhostEl.textContent = `VHost: ${selectedQueue.vhost}`;
  queueDlxEl.textContent = selectedQueue.deadLetterExchange
    ? `DLX Exchange: ${selectedQueue.deadLetterExchange}`
    : 'DLX Queue mapping';
  queueReadyEl.textContent = selectedDlxQueue
    ? `DLX ready: ${selectedDlxQueue.messagesReady}`
    : `Total ready: ${selectedQueue.messagesReady}`;

  dlxSelectEl.innerHTML = '';
  selectedQueue.dlxQueues.forEach((dlx) => {
    const option = document.createElement('option');
    option.value = dlx.name;
    const labelParts = [dlx.name];
    if (dlx.routingKey) {
      labelParts.push(`rk: ${dlx.routingKey}`);
    }
    if (typeof dlx.messagesReady === 'number') {
      labelParts.push(`${dlx.messagesReady} ready`);
    }
    option.textContent = labelParts.join(' • ');
    if (selectedDlxQueue && selectedDlxQueue.name === dlx.name) {
      option.selected = true;
    }
    dlxSelectEl.appendChild(option);
  });

  updateBulkActions();
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
    updateBulkActions();
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
    payload.textContent = safeStringify(getDisplayPayload(message));

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
      navigator.clipboard.writeText(getDisplayPayload(message) || '');
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

  updateBulkActions();
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
    await loadQueues({ preserveSelection: true, silent: true });
    await loadMessages();
  } catch (error) {
    console.error(error);
    setStatus(`Error moving message: ${error.message}`);
  } finally {
    button.disabled = false;
    button.textContent = previousText;
  }
}

async function handleMoveAllMessages(button) {
  const previousText = button.textContent;
  button.disabled = true;
  button.textContent = 'Moving...';
  setStatus('Moving all messages back to processing queue...');

  try {
    await window.api.moveAllMessages({
      vhost: state.selectedQueue.vhost,
      dlxQueue: state.selectedDlxQueue.name,
      targetQueue: state.selectedQueue.name,
      targetRoutingKey: state.selectedDlxQueue.routingKey
    });
    setStatus('All messages moved. Refreshing...');
    await loadQueues({ preserveSelection: true, silent: true });
    await loadMessages();
  } catch (error) {
    console.error(error);
    setStatus(`Error moving messages: ${error.message}`);
  } finally {
    button.textContent = previousText;
    button.disabled = false;
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
  updateBulkActions();
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
  if (value === undefined || value === null) return '';
  if (typeof value !== 'string') {
    try {
      return JSON.stringify(value, null, 2);
    } catch (error) {
      return String(value);
    }
  }

  try {
    const parsed = JSON.parse(value);
    return JSON.stringify(parsed, null, 2);
  } catch (error) {
    return value;
  }
}

function getDisplayPayload(message) {
  if (message.payloadText !== undefined && message.payloadText !== null && message.payloadText !== '') {
    return message.payloadText;
  }

  if (message.payloadEncoding === 'base64' && message.payloadBase64) {
    try {
      return atob(message.payloadBase64);
    } catch (error) {
      console.error('Failed to decode payload', error);
      return message.payloadBase64;
    }
  }

  if (message.payload !== undefined) {
    return message.payload;
  }

  return '';
}

function updateBulkActions() {
  moveAllBtn.disabled =
    !state.messages.length || !state.selectedQueue || !state.selectedDlxQueue;
}

function handleFilteredSelection() {
  if (!state.selectedQueue) {
    return;
  }

  const visibleQueues = getFilteredQueues();
  const isSelectedVisible = visibleQueues.some(
    (queue) => queue.name === state.selectedQueue.name && queue.vhost === state.selectedQueue.vhost
  );

  if (!isSelectedVisible) {
    state.selectedQueue = null;
    state.selectedDlxQueue = null;
    state.messages = [];
    renderQueueSummary();
    renderMessages();
    toggleQueueDetails(false);
    renderQueueList();
  }
}

document.addEventListener('DOMContentLoaded', () => {
  initialize().catch((error) => {
    console.error(error);
    setStatus(`Initialization error: ${error.message}`);
  });
});
