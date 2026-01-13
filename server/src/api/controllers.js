const queueManager = require('../queue/queueManager');
const { logger } = require('../logging/logger');

/**
 * API Controllers
 * Handle HTTP request logic
 * Never execute jobs directly
 */

/**
 * Create a new email job
 * POST /jobs/email
 */
async function createEmailJob(req, res) {
  try {
    const { to, subject, body } = req.body;

    // Validate required fields
    if (!to || !subject || !body) {
      return res.status(400).json({
        error: 'Missing required fields',
        required: ['to', 'subject', 'body']
      });
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(to)) {
      return res.status(400).json({
        error: 'Invalid email address format'
      });
    }

    // Create job in queue
    const job = queueManager.createJob('SEND_EMAIL', {
      to,
      subject,
      body
    });

    // Return job details immediately
    return res.status(201).json({
      jobId: job.id,
      status: job.status,
      createdAt: job.createdAt,
      message: 'Email job created successfully'
    });
  } catch (error) {
    logger.error({ 
      event: 'api:create-job-error', 
      error: error.message 
    });

    // Handle queue full error
    if (error.message.includes('maximum capacity')) {
      return res.status(503).json({
        error: 'Queue is at maximum capacity',
        message: 'Please try again later'
      });
    }

    return res.status(500).json({
      error: 'Failed to create job',
      message: error.message
    });
  }
}

/**
 * Get job status by ID
 * GET /jobs/:id
 */
async function getJobStatus(req, res) {
  try {
    const { id } = req.params;

    const job = queueManager.getJob(id);

    if (!job) {
      return res.status(404).json({
        error: 'Job not found',
        jobId: id
      });
    }

    // Return job details
    return res.status(200).json({
      jobId: job.id,
      type: job.type,
      status: job.status,
      attempts: job.attempts,
      maxAttempts: job.maxAttempts,
      createdAt: job.createdAt,
      startedAt: job.startedAt,
      finishedAt: job.finishedAt,
      error: job.error,
      payload: job.payload
    });
  } catch (error) {
    logger.error({ 
      event: 'api:get-job-error', 
      error: error.message 
    });

    return res.status(500).json({
      error: 'Failed to retrieve job',
      message: error.message
    });
  }
}

/**
 * Health check endpoint
 * GET /health
 */
async function healthCheck(req, res) {
  try {
    const queueStats = queueManager.getStats();

    return res.status(200).json({
      status: 'healthy',
      timestamp: Date.now(),
      queue: queueStats,
      process: {
        pid: process.pid,
        uptime: process.uptime(),
        memoryUsage: process.memoryUsage()
      }
    });
  } catch (error) {
    logger.error({ 
      event: 'api:health-check-error', 
      error: error.message 
    });

    return res.status(500).json({
      status: 'unhealthy',
      error: error.message
    });
  }
}

module.exports = {
  createEmailJob,
  getJobStatus,
  healthCheck
};