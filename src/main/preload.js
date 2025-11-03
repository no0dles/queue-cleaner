const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('api', {
  listQueues: () => ipcRenderer.invoke('rabbitmq:listQueues'),
  getMessages: (params) => ipcRenderer.invoke('rabbitmq:getMessages', params),
  moveMessage: (params) => ipcRenderer.invoke('rabbitmq:moveMessage', params),
  testConnection: () => ipcRenderer.invoke('rabbitmq:testConnection')
});
