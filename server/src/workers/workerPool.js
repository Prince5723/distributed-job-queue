const { Worker } = require('worker_threads');
const path = require('path');
const { logger, logWorkerEvent } = require('../logging/logger');

/**
 * Worker Pool
 * Manages a fixed pool of reusable worker threads
 * Distributes jobs across available workers
 * Handles worker lifecycle and failures
 */
class WorkerPool {
  constructor(poolSize) {
    this.poolSize = poolSize || parseInt(process.env.WORKER_POOL_SIZE || '4', 10);
    this.workers = [];
    this.availableWorkers = [];
    this.busyWorkers = new Map(); // workerId -> jobId
    this.jobCallbacks = new Map(); // jobId -> { resolve, reject }
    this.isShuttingDown = false;
  }

  /**
   * Initialize the worker pool
   * Creates all workers upfront
   */
  async initialize() {
    logger.info({ 
      event: 'worker-pool:initializing', 
      poolSize: this.poolSize 
    });

    const workerPath = path.join(__dirname, 'worker.js');

    // Create all workers
    for (let i = 0; i < this.poolSize; i++) {
      await this.createWorker(workerPath);
    }

    logger.info({ 
      event: 'worker-pool:initialized', 
      totalWorkers: this.workers.length 
    });
  }

  /**
   * Create a single worker
   * @param {string} workerPath - Path to worker script
   */
  async createWorker(workerPath) {
    return new Promise((resolve, reject) => {
      const worker = new Worker(workerPath);
      const workerInfo = {
        id: worker.threadId,
        worker,
        currentJob: null
      };

      // Handle messages from worker
      worker.on('message', (message) => {
        this.handleWorkerMessage(workerInfo, message);
      });

      // Handle worker errors
      worker.on('error', (error) => {
        this.handleWorkerError(workerInfo, error);
      });

      // Handle worker exit
      worker.on('exit', (code) => {
        this.handleWorkerExit(workerInfo, code);
      });

      // Wait for worker ready signal
      const readyHandler = (message) => {
        if (message.type === 'ready') {
          this.workers.push(workerInfo);
          this.availableWorkers.push(workerInfo);
          logWorkerEvent('worker:ready', worker.threadId);
          resolve(workerInfo);
        }
      };

      worker.once('message', readyHandler);

      // Timeout if worker doesn't become ready
      setTimeout(() => {
        worker.off('message', readyHandler);
        reject(new Error(`Worker ${worker.threadId} failed to initialize`));
      }, 5000);
    });
  }

  /**
   * Handle messages from worker threads
   * @param {Object} workerInfo
   * @param {Object} message
   */
  handleWorkerMessage(workerInfo, message) {
    switch (message.type) {
      case 'success':
        this.handleJobSuccess(workerInfo, message);
        break;
      case 'failure':
        this.handleJobFailure(workerInfo, message);
        break;
      case 'worker-error':
        logWorkerEvent('worker:error', workerInfo.id, { error: message.error });
        break;
      case 'terminated':
        logWorkerEvent('worker:terminated', workerInfo.id);
        break;
    }
  }

  /**
   * Handle successful job execution
   * @param {Object} workerInfo
   * @param {Object} message
   */
  handleJobSuccess(workerInfo, message) {
    const { jobId, result } = message;
    const callback = this.jobCallbacks.get(jobId);

    if (callback) {
      callback.resolve(result);
      this.jobCallbacks.delete(jobId);
    }

    // Mark worker as available again
    this.releaseWorker(workerInfo);
  }

  /**
   * Handle failed job execution
   * @param {Object} workerInfo
   * @param {Object} message
   */
  handleJobFailure(workerInfo, message) {
    const { jobId, error } = message;
    const callback = this.jobCallbacks.get(jobId);

    if (callback) {
      callback.reject(new Error(error.message));
      this.jobCallbacks.delete(jobId);
    }

    // Mark worker as available again
    this.releaseWorker(workerInfo);
  }

  /**
   * Handle worker errors
   * @param {Object} workerInfo
   * @param {Error} error
   */
  handleWorkerError(workerInfo, error) {
    logWorkerEvent('worker:error', workerInfo.id, { 
      error: error.message,
      currentJob: workerInfo.currentJob
    });

    // If worker had a job, fail it
    if (workerInfo.currentJob) {
      const callback = this.jobCallbacks.get(workerInfo.currentJob);
      if (callback) {
        callback.reject(new Error(`Worker crashed: ${error.message}`));
        this.jobCallbacks.delete(workerInfo.currentJob);
      }
    }

    // Remove worker from pool
    this.removeWorker(workerInfo);

    // Replace worker if not shutting down
    if (!this.isShuttingDown) {
      const workerPath = path.join(__dirname, 'worker.js');
      this.createWorker(workerPath).catch(err => {
        logger.error({ 
          event: 'worker:replace-failed', 
          error: err.message 
        });
      });
    }
  }

  /**
   * Handle worker exit
   * @param {Object} workerInfo
   * @param {number} code
   */
  handleWorkerExit(workerInfo, code) {
    logWorkerEvent('worker:exit', workerInfo.id, { exitCode: code });

    // If worker had a job, fail it
    if (workerInfo.currentJob) {
      const callback = this.jobCallbacks.get(workerInfo.currentJob);
      if (callback) {
        callback.reject(new Error(`Worker exited with code ${code}`));
        this.jobCallbacks.delete(workerInfo.currentJob);
      }
    }

    // Remove worker from pool
    this.removeWorker(workerInfo);
  }

  /**
   * Execute a job on an available worker
   * @param {Object} job - Job to execute
   * @returns {Promise} Resolves when job completes
   */
  async executeJob(job) {
    if (this.isShuttingDown) {
      throw new Error('Worker pool is shutting down');
    }

    // Get an available worker
    const workerInfo = await this.getAvailableWorker();

    return new Promise((resolve, reject) => {
      // Store callbacks for this job
      this.jobCallbacks.set(job.id, { resolve, reject });

      // Mark worker as busy
      workerInfo.currentJob = job.id;
      this.busyWorkers.set(workerInfo.id, job.id);

      // Send job to worker
      workerInfo.worker.postMessage({
        type: 'execute',
        job
      });

      logWorkerEvent('worker:job-assigned', workerInfo.id, { jobId: job.id });
    });
  }

  /**
   * Get an available worker
   * Waits if all workers are busy
   * @returns {Promise<Object>} Worker info
   */
  async getAvailableWorker() {
    // If worker available, return immediately
    if (this.availableWorkers.length > 0) {
      return this.availableWorkers.shift();
    }

    // Wait for a worker to become available
    return new Promise((resolve) => {
      const checkInterval = setInterval(() => {
        if (this.availableWorkers.length > 0) {
          clearInterval(checkInterval);
          resolve(this.availableWorkers.shift());
        }
      }, 100);
    });
  }

  /**
   * Release worker back to available pool
   * @param {Object} workerInfo
   */
  releaseWorker(workerInfo) {
    workerInfo.currentJob = null;
    this.busyWorkers.delete(workerInfo.id);
    
    // Only add back to available if worker still exists and not shutting down
    if (this.workers.includes(workerInfo) && !this.isShuttingDown) {
      this.availableWorkers.push(workerInfo);
      logWorkerEvent('worker:released', workerInfo.id);
    }
  }

  /**
   * Remove worker from pool
   * @param {Object} workerInfo
   */
  removeWorker(workerInfo) {
    const index = this.workers.indexOf(workerInfo);
    if (index > -1) {
      this.workers.splice(index, 1);
    }

    const availableIndex = this.availableWorkers.indexOf(workerInfo);
    if (availableIndex > -1) {
      this.availableWorkers.splice(availableIndex, 1);
    }

    this.busyWorkers.delete(workerInfo.id);
  }

  /**
   * Check if any workers are busy
   * @returns {boolean}
   */
  hasBusyWorkers() {
    return this.busyWorkers.size > 0;
  }

  /**
   * Get pool statistics
   * @returns {Object}
   */
  getStats() {
    return {
      total: this.workers.length,
      available: this.availableWorkers.length,
      busy: this.busyWorkers.size
    };
  }

  /**
   * Gracefully shutdown worker pool
   * Waits for all jobs to complete
   */
  async shutdown() {
    this.isShuttingDown = true;
    logger.info({ event: 'worker-pool:shutdown-started' });

    // Wait for all busy workers to finish
    while (this.hasBusyWorkers()) {
      logger.info({ 
        event: 'worker-pool:waiting-for-workers', 
        busyCount: this.busyWorkers.size 
      });
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // Terminate all workers
    const terminatePromises = this.workers.map(workerInfo => {
      return workerInfo.worker.terminate();
    });

    await Promise.all(terminatePromises);

    logger.info({ event: 'worker-pool:shutdown-complete' });
  }
}

module.exports = WorkerPool;