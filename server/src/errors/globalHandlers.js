const { logError, logger } = require('../logging/logger');

/**
 * Global Error Handlers
 * Handles uncaught exceptions and unhandled rejections
 * Ensures errors are logged and process remains stable
 */

/**
 * Setup global error handlers
 */
function setupGlobalErrorHandlers() {
  /**
   * Handle uncaught exceptions
   * These are synchronous errors that weren't caught
   */
  process.on('uncaughtException', (error, origin) => {
    logError(error, {
      event: 'uncaughtException',
      origin,
      fatal: true
    });

    // In production, we might want to restart the process
    // For now, we log but don't exit unless it's truly unrecoverable
    if (isUnrecoverableError(error)) {
      logger.error({
        event: 'process:unrecoverable-error',
        message: 'Unrecoverable error detected, exiting'
      });
      process.exit(1);
    }
  });

  /**
   * Handle unhandled promise rejections
   * These are promises that rejected without a .catch()
   */
  process.on('unhandledRejection', (reason, promise) => {
    const error = reason instanceof Error ? reason : new Error(String(reason));
    
    logError(error, {
      event: 'unhandledRejection',
      promise: promise.toString()
    });

    // Don't exit - log and continue
    // The scheduler and worker pool will handle job-level errors
  });

  /**
   * Handle warnings
   */
  process.on('warning', (warning) => {
    logger.warn({
      event: 'process:warning',
      name: warning.name,
      message: warning.message,
      stack: warning.stack
    });
  });

  logger.info({ event: 'global-error-handlers:initialized' });
}

/**
 * Determine if an error is unrecoverable
 * These errors should cause process exit
 * @param {Error} error
 * @returns {boolean}
 */
function isUnrecoverableError(error) {
  const unrecoverableErrors = [
    'ENOSPC', // No space left on device
    'EMFILE', // Too many open files
    'ENOMEM'  // Out of memory
  ];

  return unrecoverableErrors.includes(error.code);
}

/**
 * Create error handler for specific contexts
 * @param {string} context - Context name for logging
 * @returns {Function} Error handler function
 */
function createErrorHandler(context) {
  return (error) => {
    logError(error, { context });
  };
}

module.exports = {
  setupGlobalErrorHandlers,
  createErrorHandler,
  isUnrecoverableError
};