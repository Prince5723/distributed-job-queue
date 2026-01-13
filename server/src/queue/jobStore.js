const { v4: uuidv4 } = require('uuid');

/**
 * In-memory job storage
 * Maintains all jobs and their states
 * Thread-safe operations for concurrent access
 */
class JobStore {
  constructor() {
    // Map of jobId -> job object
    this.jobs = new Map();
    
    // Track jobs by status for quick queries
    this.jobsByStatus = {
      PENDING: new Set(),
      RUNNING: new Set(),
      COMPLETED: new Set(),
      FAILED: new Set(),
      RETRYING: new Set(),
      DEAD: new Set()
    };

    this.maxQueueSize = parseInt(process.env.QUEUE_MAX_SIZE || '10000', 10);
  }

  /**
   * Create a new job
   * @param {string} type - Job type
   * @param {Object} payload - Job payload data
   * @param {Object} options - Additional job options
   * @returns {Object} Created job
   */
  createJob(type, payload, options = {}) {
    // Check if queue is at capacity
    if (this.jobs.size >= this.maxQueueSize) {
      throw new Error('Queue is at maximum capacity');
    }

    const job = {
      id: uuidv4(),
      type,
      payload,
      status: 'PENDING',
      attempts: 0,
      maxAttempts: options.maxAttempts || parseInt(process.env.MAX_JOB_ATTEMPTS || '3', 10),
      createdAt: Date.now(),
      startedAt: null,
      finishedAt: null,
      error: null
    };

    this.jobs.set(job.id, job);
    this.jobsByStatus.PENDING.add(job.id);

    return job;
  }

  /**
   * Get job by ID
   * @param {string} jobId
   * @returns {Object|null} Job object or null
   */
  getJob(jobId) {
    return this.jobs.get(jobId) || null;
  }

  /**
   * Update job status
   * @param {string} jobId
   * @param {string} newStatus
   * @param {Object} updates - Additional fields to update
   */
  updateJobStatus(jobId, newStatus, updates = {}) {
    const job = this.jobs.get(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }

    // Remove from old status set
    this.jobsByStatus[job.status].delete(jobId);

    // Update job
    job.status = newStatus;
    Object.assign(job, updates);

    // Add to new status set
    this.jobsByStatus[newStatus].add(jobId);

    return job;
  }

  /**
   * Mark job as started
   * @param {string} jobId
   */
  markJobStarted(jobId) {
    return this.updateJobStatus(jobId, 'RUNNING', {
      startedAt: Date.now(),
      attempts: this.getJob(jobId).attempts + 1
    });
  }

  /**
   * Mark job as completed
   * @param {string} jobId
   */
  markJobCompleted(jobId) {
    return this.updateJobStatus(jobId, 'COMPLETED', {
      finishedAt: Date.now()
    });
  }

  /**
   * Mark job as failed
   * @param {string} jobId
   * @param {string} error
   */
  markJobFailed(jobId, error) {
    const job = this.getJob(jobId);
    
    // If max attempts reached, mark as DEAD
    if (job.attempts >= job.maxAttempts) {
      return this.updateJobStatus(jobId, 'DEAD', {
        finishedAt: Date.now(),
        error
      });
    }

    // Otherwise mark as FAILED (will be retried)
    return this.updateJobStatus(jobId, 'FAILED', {
      error
    });
  }

  /**
   * Mark job as retrying
   * @param {string} jobId
   * @param {number} retryAt - Timestamp when job should be retried
   */
  markJobRetrying(jobId, retryAt) {
    return this.updateJobStatus(jobId, 'RETRYING', {
      retryAt
    });
  }

  /**
   * Get all jobs with specific status
   * @param {string} status
   * @returns {Array} Array of jobs
   */
  getJobsByStatus(status) {
    const jobIds = Array.from(this.jobsByStatus[status]);
    return jobIds.map(id => this.jobs.get(id)).filter(Boolean);
  }

  /**
   * Get jobs ready for execution (PENDING or RETRYING with elapsed delay)
   * @returns {Array} Array of jobs ready to run
   */
  getJobsReadyForExecution() {
    const now = Date.now();
    const jobs = [];

    // Get all PENDING jobs
    const pendingJobs = this.getJobsByStatus('PENDING');
    jobs.push(...pendingJobs);

    // Get RETRYING jobs whose retry delay has elapsed
    const retryingJobs = this.getJobsByStatus('RETRYING').filter(job => {
      return job.retryAt && job.retryAt <= now;
    });
    jobs.push(...retryingJobs);

    return jobs;
  }

  /**
   * Get queue statistics
   * @returns {Object} Queue stats
   */
  getStats() {
    return {
      total: this.jobs.size,
      pending: this.jobsByStatus.PENDING.size,
      running: this.jobsByStatus.RUNNING.size,
      completed: this.jobsByStatus.COMPLETED.size,
      failed: this.jobsByStatus.FAILED.size,
      retrying: this.jobsByStatus.RETRYING.size,
      dead: this.jobsByStatus.DEAD.size
    };
  }

  /**
   * Check if a job is currently running
   * Prevents duplicate execution
   * @param {string} jobId
   * @returns {boolean}
   */
  isJobRunning(jobId) {
    const job = this.getJob(jobId);
    return job && job.status === 'RUNNING';
  }
}

module.exports = JobStore;