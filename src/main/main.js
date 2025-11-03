const { app, BrowserWindow, ipcMain } = require('electron');
const path = require('path');
const RabbitMQService = require('./rabbitmqService');

const rabbitService = new RabbitMQService();

function createWindow() {
  const mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  mainWindow.loadFile(path.join(__dirname, '../renderer/index.html'));
}

app.whenReady().then(() => {
  createWindow();

  ipcMain.handle('rabbitmq:listQueues', async () => {
    return rabbitService.listQueuesWithDLX();
  });

  ipcMain.handle('rabbitmq:getMessages', async (_event, payload) => {
    const { vhost, queue, count } = payload;
    return rabbitService.getMessages({ vhost, queue, count });
  });

  ipcMain.handle('rabbitmq:moveMessage', async (_event, payload) => {
    return rabbitService.moveMessage(payload);
  });

  ipcMain.handle('rabbitmq:moveAllMessages', async (_event, payload) => {
    return rabbitService.moveAllMessages(payload);
  });

  ipcMain.handle('rabbitmq:testConnection', async () => {
    await rabbitService.ensureChannel();
    return true;
  });

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on('window-all-closed', async () => {
  if (process.platform !== 'darwin') {
    await rabbitService.close();
    app.quit();
  }
});
