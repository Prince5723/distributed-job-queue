const EventEmitter = require('events');
const { logJobEvent } = require('../logging/logger');

/**
 * Central event emitter for job lifecycle events
 * Emits events that other components can listen to
 */
class JobEventEmitter extends EventEmitter {
  constructor() {
    super();
    this.setupEventLogging();
  }

  /**
   * Setup automatic logging for all job events
   * This ensures every lifecycle event is captured in logs
   */
  setupEventLogging() {
    // Log all job lifecycle events
    this.on('job:created', (job) => {
      logJobEvent('job:created', job);
    });

    this.on('job:started', (job) => {
      logJobEvent('job:started', job, { startedAt: job.startedAt });
    });

    this.on('job:completed', (job) => {
      logJobEvent('job:completed', job, { 
        finishedAt: job.finishedAt,
        duration: job.finishedAt - job.startedAt
      });
    });

    this.on('job:failed', (job) => {
      logJobEvent('job:failed', job, { 
        error: job.error,
        attempts: job.attempts
      });
    });

    this.on('job:retrying', (job, retryDelay) => {
      logJobEvent('job:retrying', job, { 
        retryDelay,
        nextAttempt: job.attempts + 1
      });
    });

    this.on('job:dead', (job) => {
      logJobEvent('job:dead', job, { 
        finalError: job.error,
        totalAttempts: job.attempts
      });
    });
  }

  /**
   * Emit job created event
   */
  emitJobCreated(job) {
    this.emit('job:created', job);
  }

  /**
   * Emit job started event
   */
  emitJobStarted(job) {
    this.emit('job:started', job);
  }

  /**
   * Emit job completed event
   */
  emitJobCompleted(job) {
    this.emit('job:completed', job);
  }

  /**
   * Emit job failed event
   */
  emitJobFailed(job) {
    this.emit('job:failed', job);
  }

  /**
   * Emit job retrying event
   */
  emitJobRetrying(job, retryDelay) {
    this.emit('job:retrying', job, retryDelay);
  }

  /**
   * Emit job dead event (max retries exceeded)
   */
  emitJobDead(job) {
    this.emit('job:dead', job);
  }
}

// Export singleton instance
const jobEvents = new JobEventEmitter();

module.exports = jobEvents;