const express = require('express');
const controllers = require('./controllers');

/**
 * API Routes
 * Defines all HTTP endpoints
 */

function routes() {
  const router = express.Router();

  // POST /jobs/email - Create a new email job
  router.post('/jobs/email', controllers.createEmailJob);

  // GET /jobs/:id - Get job status
  router.get('/jobs/:id', controllers.getJobStatus);

  // GET /health - Health check
  router.get('/health', controllers.healthCheck);

  return router;
}

module.exports = routes;