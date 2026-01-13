const express = require('express');
const pinoHttp = require('pino-http');
const routes = require('./routes');
const { logger } = require('../logging/logger');

/**
 * Create and configure Express server
 * Runs in cluster worker processes
 * Never executes jobs
 */
function createServer() {
  const app = express();

  // Parse JSON request bodies
  app.use(express.json());

  // Parse URL-encoded bodies
  app.use(express.urlencoded({ extended: true }));

  // Add pino HTTP logging middleware
  app.use(pinoHttp({
    logger,
    customLogLevel: function (req, res, err) {
      if (res.statusCode >= 400 && res.statusCode < 500) {
        return 'warn';
      } else if (res.statusCode >= 500 || err) {
        return 'error';
      }
      return 'info';
    },
    customSuccessMessage: function (req, res) {
      return `${req.method} ${req.url} completed`;
    },
    customErrorMessage: function (req, res, err) {
      return `${req.method} ${req.url} failed: ${err.message}`;
    }
  }));

  // Register routes
  app.use('/', routes());

  // 404 handler
  app.use((req, res) => {
    res.status(404).json({
      error: 'Not Found',
      message: `Route ${req.method} ${req.url} not found`
    });
  });

  // Global error handler
  app.use((err, req, res, next) => {
    logger.error({
      event: 'http:error',
      error: err.message,
      stack: err.stack,
      method: req.method,
      url: req.url
    });

    res.status(err.statusCode || 500).json({
      error: err.message || 'Internal Server Error',
      statusCode: err.statusCode || 500
    });
  });

  return app;
}

/**
 * Start the server
 * @param {Object} app - Express app instance
 * @param {number} port - Port to listen on
 */
async function startServer(app, port) {
  return new Promise((resolve, reject) => {
    try {
      const server = app.listen(port, '0.0.0.0', () => {
        logger.info({
          event: 'server:started',
          port,
          pid: process.pid,
          worker: process.env.WORKER_ID || 'master'
        });
        resolve(server);
      });

      server.on('error', (error) => {
        logger.error({
          event: 'server:start-error',
          error: error.message
        });
        reject(error);
      });
    } catch (error) {
      logger.error({
        event: 'server:start-error',
        error: error.message
      });
      reject(error);
    }
  });
}

/**
 * Stop the server gracefully
 * @param {Object} server - Express server instance
 */
async function stopServer(server) {
  return new Promise((resolve, reject) => {
    try {
      logger.info({ event: 'server:stopping' });
      
      // Close server (stops accepting new connections)
      server.close((err) => {
        if (err) {
          logger.error({
            event: 'server:stop-error',
            error: err.message
          });
          reject(err);
        } else {
          logger.info({ event: 'server:stopped' });
          resolve();
        }
      });
    } catch (error) {
      logger.error({
        event: 'server:stop-error',
        error: error.message
      });
      reject(error);
    }
  });
}

module.exports = {
  createServer,
  startServer,
  stopServer
};