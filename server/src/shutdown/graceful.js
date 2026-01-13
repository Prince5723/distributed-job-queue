const { logger } = require('../logging/logger');

/**
 * Graceful Shutdown Manager
 * Handles SIGINT and SIGTERM signals
 * Ensures no jobs are lost during shutdown
 * Coordinates shutdown across all components
 */
class GracefulShutdown {
  constructor() {
    this.isShuttingDown = false;
    this.shutdownHandlers = [];
    this.shutdownTimeout = 30000; // 30 seconds
  }

  /**
   * Register a shutdown handler
   * Handlers are called in registration order
   * @param {Function} handler - Async function to call during shutdown
   * @param {string} name - Handler name for logging
   */
  registerHandler(handler, name) {
    this.shutdownHandlers.push({ handler, name });
  }

  /**
   * Setup signal handlers for graceful shutdown
   */
  setupSignalHandlers() {
    // Handle SIGINT (Ctrl+C)
    process.on('SIGINT', () => {
      logger.info({ event: 'shutdown:signal-received', signal: 'SIGINT' });
      this.shutdown('SIGINT');
    });

    // Handle SIGTERM (process termination)
    process.on('SIGTERM', () => {
      logger.info({ event: 'shutdown:signal-received', signal: 'SIGTERM' });
      this.shutdown('SIGTERM');
    });
  }

  /**
   * Execute graceful shutdown
   * @param {string} signal - Signal that triggered shutdown
   */
  async shutdown(signal) {
    // Prevent multiple shutdown attempts
    if (this.isShuttingDown) {
      logger.warn({ event: 'shutdown:already-in-progress' });
      return;
    }

    this.isShuttingDown = true;

    logger.info({
      event: 'shutdown:started',
      signal,
      handlers: this.shutdownHandlers.length
    });

    // Set timeout to force exit if graceful shutdown takes too long
    const forceExitTimer = setTimeout(() => {
      logger.error({
        event: 'shutdown:timeout',
        message: 'Graceful shutdown timeout exceeded, forcing exit'
      });
      process.exit(1);
    }, this.shutdownTimeout);

    try {
      // Execute all shutdown handlers in order
      for (const { handler, name } of this.shutdownHandlers) {
        logger.info({ event: 'shutdown:executing-handler', handler: name });
        
        try {
          await handler();
          logger.info({ event: 'shutdown:handler-complete', handler: name });
        } catch (error) {
          logger.error({
            event: 'shutdown:handler-error',
            handler: name,
            error: error.message
          });
        }
      }

      logger.info({ event: 'shutdown:complete' });
      
      // Clear force exit timer
      clearTimeout(forceExitTimer);

      // Exit cleanly
      process.exit(0);
    } catch (error) {
      logger.error({
        event: 'shutdown:error',
        error: error.message,
        stack: error.stack
      });

      clearTimeout(forceExitTimer);
      process.exit(1);
    }
  }

  /**
   * Check if system is shutting down
   * @returns {boolean}
   */
  isShutdown() {
    return this.isShuttingDown;
  }
}

// Export singleton instance
module.exports = new GracefulShutdown();