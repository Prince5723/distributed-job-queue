const queueManager = require('./queueManager');
const { logger } = require('../logging/logger');

/**
 * Scheduler
 * Continuously pulls jobs ready for execution
 * Dispatches jobs to worker pool
 * Honors retry delays
 * Pausable during graceful shutdown
 */
class Scheduler {
  constructor(workerPool) {
    this.workerPool = workerPool;
    this.isRunning = false;
    this.isPaused = false;
    this.pollInterval = 1000; // Poll every second
    this.schedulerTimer = null;
    this.activeExecutions = new Set(); // Track jobs being executed
  }

  /**
   * Start the scheduler
   * Begins polling for jobs and dispatching to workers
   */
  start() {
    if (this.isRunning) {
      logger.warn({ event: 'scheduler:already-running' });
      return;
    }

    this.isRunning = true;
    this.isPaused = false;
    logger.info({ event: 'scheduler:started' });

    this.scheduleNextPoll();
  }

  /**
   * Schedule the next poll cycle
   */
  scheduleNextPoll() {
    if (!this.isRunning) {
      return;
    }

    this.schedulerTimer = setTimeout(() => {
      this.poll();
    }, this.pollInterval);
  }

  /**
   * Poll for jobs ready to execute
   * Dispatches jobs to worker pool
   */
  async poll() {
    // Skip if paused or shutting down
    if (this.isPaused || !this.isRunning) {
      return;
    }

    try {
      // Get jobs ready for execution
      const jobs = queueManager.getJobsReadyForExecution();

      if (jobs.length > 0) {
        logger.info({ 
          event: 'scheduler:jobs-found', 
          count: jobs.length 
        });

        // Dispatch each job
        for (const job of jobs) {
          // Skip if already being executed (prevent duplicates)
          if (this.activeExecutions.has(job.id)) {
            continue;
          }

          // Dispatch to worker pool (non-blocking)
          this.dispatchJob(job);
        }
      }
    } catch (error) {
      logger.error({ 
        event: 'scheduler:poll-error', 
        error: error.message 
      });
    }

    // Schedule next poll
    this.scheduleNextPoll();
  }

  /**
   * Dispatch a job to the worker pool
   * Handles job lifecycle based on execution result
   * @param {Object} job
   */
  async dispatchJob(job) {
    // Mark job as being executed
    this.activeExecutions.add(job.id);

    try {
      // Update job status to RUNNING
      queueManager.startJob(job.id);

      // Execute job on worker pool
      await this.workerPool.executeJob(job);

      // Job succeeded - mark as completed
      queueManager.completeJob(job.id);
    } catch (error) {
      // Job failed - handle retry logic
      logger.error({ 
        event: 'scheduler:job-execution-failed', 
        jobId: job.id,
        error: error.message 
      });

      queueManager.failJob(job.id, error.message);
    } finally {
      // Remove from active executions
      this.activeExecutions.delete(job.id);
    }
  }

  /**
   * Pause the scheduler
   * Stops polling for new jobs
   * Does not affect jobs already being executed
   */
  pause() {
    if (!this.isRunning) {
      return;
    }

    this.isPaused = true;
    
    // Clear pending poll timer
    if (this.schedulerTimer) {
      clearTimeout(this.schedulerTimer);
      this.schedulerTimer = null;
    }

    logger.info({ event: 'scheduler:paused' });
  }

  /**
   * Resume the scheduler
   * Resumes polling for jobs
   */
  resume() {
    if (!this.isRunning || !this.isPaused) {
      return;
    }

    this.isPaused = false;
    logger.info({ event: 'scheduler:resumed' });

    this.scheduleNextPoll();
  }

  /**
   * Stop the scheduler completely
   */
  stop() {
    this.isRunning = false;
    this.isPaused = false;

    if (this.schedulerTimer) {
      clearTimeout(this.schedulerTimer);
      this.schedulerTimer = null;
    }

    logger.info({ event: 'scheduler:stopped' });
  }

  /**
   * Check if scheduler has active executions
   * Used during graceful shutdown
   * @returns {boolean}
   */
  hasActiveExecutions() {
    return this.activeExecutions.size > 0;
  }

  /**
   * Get scheduler statistics
   * @returns {Object}
   */
  getStats() {
    return {
      isRunning: this.isRunning,
      isPaused: this.isPaused,
      activeExecutions: this.activeExecutions.size
    };
  }

  /**
   * Wait for all active executions to complete
   * Used during graceful shutdown
   */
  async waitForActiveExecutions() {
    while (this.hasActiveExecutions()) {
      logger.info({ 
        event: 'scheduler:waiting-for-executions', 
        count: this.activeExecutions.size 
      });
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}

module.exports = Scheduler;