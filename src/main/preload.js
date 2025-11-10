const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  listQueues: (params = {}) => ipcRenderer.invoke('rabbitmq:listQueues', params),
  getEnvironments: () => ipcRenderer.invoke('rabbitmq:getEnvironments'),
  getMessages: (params) => ipcRenderer.invoke('rabbitmq:getMessages', params),
  moveMessage: (params) => ipcRenderer.invoke('rabbitmq:moveMessage', params),
  moveAllMessages: (params) => ipcRenderer.invoke('rabbitmq:moveAllMessages', params),
  testConnection: (params = {}) => ipcRenderer.invoke('rabbitmq:testConnection', params)
});
