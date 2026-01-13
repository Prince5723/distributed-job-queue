const pino = require('pino');

// Create a pino logger instance with structured logging
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: process.env.NODE_ENV === 'development' ? {
    target: 'pino-pretty',
    options: {
      colorize: true,
      translateTime: 'HH:MM:ss Z',
      ignore: 'pid,hostname'
    }
  } : undefined,
  // Base fields included in every log
  base: {
    pid: process.pid,
    env: process.env.NODE_ENV || 'development'
  },
  // Always include timestamp
  timestamp: pino.stdTimeFunctions.isoTime
});

/**
 * Creates a child logger with additional context
 * @param {Object} bindings - Additional fields to include in all logs
 * @returns {Object} Child logger instance
 */
function createChildLogger(bindings) {
  return logger.child(bindings);
}

/**
 * Log job-related events with standardized structure
 * @param {string} event - Event name (e.g., 'job:created')
 * @param {Object} job - Job object
 * @param {Object} additional - Additional fields
 */
function logJobEvent(event, job, additional = {}) {
  logger.info({
    event,
    jobId: job.id,
    jobType: job.type,
    jobStatus: job.status,
    attempts: job.attempts,
    timestamp: Date.now(),
    ...additional
  });
}

/**
 * Log worker-related events
 * @param {string} event - Event name
 * @param {number} workerId - Worker thread ID
 * @param {Object} additional - Additional fields
 */
function logWorkerEvent(event, workerId, additional = {}) {
  logger.info({
    event,
    workerId,
    timestamp: Date.now(),
    ...additional
  });
}

/**
 * Log errors with full context
 * @param {Error} error - Error object
 * @param {Object} context - Additional context
 */
function logError(error, context = {}) {
  logger.error({
    event: 'error',
    error: {
      message: error.message,
      stack: error.stack,
      name: error.name
    },
    timestamp: Date.now(),
    ...context
  });
}

module.exports = {
  logger,
  createChildLogger,
  logJobEvent,
  logWorkerEvent,
  logError
};