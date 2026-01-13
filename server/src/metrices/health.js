const { logger } = require('../logging/logger');

/**
 * Health Monitoring
 * Tracks system health metrics
 * Provides health check data
 */
class HealthMonitor {
  constructor() {
    this.startTime = Date.now();
    this.metrics = {
      requestsReceived: 0,
      jobsCreated: 0,
      jobsCompleted: 0,
      jobsFailed: 0,
      jobsDead: 0
    };
  }

  /**
   * Record an HTTP request
   */
  recordRequest() {
    this.metrics.requestsReceived++;
  }

  /**
   * Record a job creation
   */
  recordJobCreated() {
    this.metrics.jobsCreated++;
  }

  /**
   * Record a job completion
   */
  recordJobCompleted() {
    this.metrics.jobsCompleted++;
  }

  /**
   * Record a job failure
   */
  recordJobFailed() {
    this.metrics.jobsFailed++;
  }

  /**
   * Record a dead job
   */
  recordJobDead() {
    this.metrics.jobsDead++;
  }

  /**
   * Get current health status
   * @param {Object} queueStats - Queue statistics
   * @param {Object} workerStats - Worker pool statistics
   * @param {Object} schedulerStats - Scheduler statistics
   * @returns {Object}
   */
  getHealthStatus(queueStats = {}, workerStats = {}, schedulerStats = {}) {
    const uptime = Date.now() - this.startTime;
    const memoryUsage = process.memoryUsage();

    return {
      status: 'healthy',
      timestamp: Date.now(),
      uptime,
      process: {
        pid: process.pid,
        uptimeSeconds: Math.floor(uptime / 1000),
        memoryUsage: {
          heapUsed: Math.round(memoryUsage.heapUsed / 1024 / 1024) + 'MB',
          heapTotal: Math.round(memoryUsage.heapTotal / 1024 / 1024) + 'MB',
          external: Math.round(memoryUsage.external / 1024 / 1024) + 'MB',
          rss: Math.round(memoryUsage.rss / 1024 / 1024) + 'MB'
        }
      },
      queue: queueStats,
      workers: workerStats,
      scheduler: schedulerStats,
      metrics: this.metrics
    };
  }

  /**
   * Get summary metrics
   * @returns {Object}
   */
  getMetrics() {
    return {
      ...this.metrics,
      successRate: this.calculateSuccessRate(),
      uptime: Date.now() - this.startTime
    };
  }

  /**
   * Calculate job success rate
   * @returns {number} Success rate as percentage
   */
  calculateSuccessRate() {
    const total = this.metrics.jobsCompleted + this.metrics.jobsDead;
    if (total === 0) return 100;
    return Math.round((this.metrics.jobsCompleted / total) * 100);
  }

  /**
   * Log health metrics periodically
   * @param {number} intervalMs - Interval in milliseconds
   */
  startPeriodicLogging(intervalMs = 60000) {
    setInterval(() => {
      logger.info({
        event: 'health:metrics',
        ...this.getMetrics()
      });
    }, intervalMs);
  }

  /**
   * Reset metrics
   */
  resetMetrics() {
    this.metrics = {
      requestsReceived: 0,
      jobsCreated: 0,
      jobsCompleted: 0,
      jobsFailed: 0,
      jobsDead: 0
    };
  }
}

// Export singleton instance
module.exports = new HealthMonitor();