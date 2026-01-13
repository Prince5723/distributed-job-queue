const JobStore = require('./jobStore');
const jobEvents = require('../events/jobEvents');
const { logger } = require('../logging/logger');

/**
 * Central Queue Manager
 * Manages job lifecycle and state transitions
 * Emits events for all state changes
 */
class QueueManager {
  constructor() {
    this.store = new JobStore();
  }

  /**
   * Create a new job and add it to the queue
   * @param {string} type - Job type
   * @param {Object} payload - Job payload
   * @param {Object} options - Job options
   * @returns {Object} Created job
   */
  createJob(type, payload, options = {}) {
    try {
      const job = this.store.createJob(type, payload, options);
      jobEvents.emitJobCreated(job);
      return job;
    } catch (error) {
      logger.error({ 
        event: 'job:create:error', 
        error: error.message,
        type,
        queueSize: this.store.jobs.size
      });
      throw error;
    }
  }

  /**
   * Get job by ID
   * @param {string} jobId
   * @returns {Object|null}
   */
  getJob(jobId) {
    return this.store.getJob(jobId);
  }

  /**
   * Start job execution
   * Updates job status to RUNNING
   * @param {string} jobId
   * @returns {Object} Updated job
   */
  startJob(jobId) {
    // Prevent duplicate execution
    if (this.store.isJobRunning(jobId)) {
      throw new Error(`Job ${jobId} is already running`);
    }

    const job = this.store.markJobStarted(jobId);
    jobEvents.emitJobStarted(job);
    return job;
  }

  /**
   * Mark job as successfully completed
   * @param {string} jobId
   * @returns {Object} Updated job
   */
  completeJob(jobId) {
    const job = this.store.markJobCompleted(jobId);
    jobEvents.emitJobCompleted(job);
    return job;
  }

  /**
   * Mark job as failed
   * Handles retry logic based on attempts
   * @param {string} jobId
   * @param {string} errorMessage
   * @returns {Object} Updated job
   */
  failJob(jobId, errorMessage) {
    const job = this.store.getJob(jobId);
    
    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }

    // Check if we should retry
    if (job.attempts < job.maxAttempts) {
      // Calculate exponential backoff delay
      const retryDelay = this.calculateRetryDelay(job.attempts);
      const retryAt = Date.now() + retryDelay;

      // Mark as FAILED first
      this.store.markJobFailed(jobId, errorMessage);
      
      // Then mark as RETRYING with retry timestamp
      const updatedJob = this.store.markJobRetrying(jobId, retryAt);
      
      jobEvents.emitJobFailed(updatedJob);
      jobEvents.emitJobRetrying(updatedJob, retryDelay);
      
      return updatedJob;
    } else {
      // Max attempts reached, mark as DEAD
      const deadJob = this.store.markJobFailed(jobId, errorMessage);
      jobEvents.emitJobFailed(deadJob);
      jobEvents.emitJobDead(deadJob);
      return deadJob;
    }
  }

  /**
   * Calculate exponential backoff delay for retries
   * Formula: base * (2 ^ attempts)
   * @param {number} attempts - Number of attempts made
   * @returns {number} Delay in milliseconds
   */
  calculateRetryDelay(attempts) {
    const baseDelay = parseInt(process.env.RETRY_BACKOFF_BASE_MS || '1000', 10);
    return baseDelay * Math.pow(2, attempts);
  }

  /**
   * Get jobs ready for execution
   * Returns PENDING jobs and RETRYING jobs whose delay has elapsed
   * @returns {Array} Jobs ready to execute
   */
  getJobsReadyForExecution() {
    return this.store.getJobsReadyForExecution();
  }

  /**
   * Get queue statistics
   * @returns {Object} Queue stats
   */
  getStats() {
    return this.store.getStats();
  }

  /**
   * Get all jobs by status
   * @param {string} status
   * @returns {Array}
   */
  getJobsByStatus(status) {
    return this.store.getJobsByStatus(status);
  }
}

// Export singleton instance
module.exports = new QueueManager();