const cluster = require('cluster');
const os = require('os');
const path = require('path');
const { logger } = require('./logging/logger');
const { setupGlobalErrorHandlers } = require('./errors/globalHandlers');
const gracefulShutdown = require('./shutdown/graceful');
const WorkerPool = require('./workers/workerPool');
const Scheduler = require('./queue/scheduler');
const { createServer, startServer, stopServer } = require('./api/server');
const jobEvents = require('./events/jobEvents');
const healthMonitor = require('./metrics/health');

// Load environment variables
require('dotenv').config();

const PORT = parseInt(process.env.PORT || '3000', 10);
const NUM_CPUS = os.cpus().length;

/**
 * Master Process
 * Manages worker pool, scheduler, and API cluster
 */
async function startMaster() {
  logger.info({
    event: 'master:starting',
    pid: process.pid,
    cpus: NUM_CPUS
  });

  // Setup global error handlers
  setupGlobalErrorHandlers();

  // Initialize worker pool for job execution
  const workerPool = new WorkerPool();
  await workerPool.initialize();

  // Initialize scheduler
  const scheduler = new Scheduler(workerPool);
  scheduler.start();

  // Setup event listeners for metrics
  jobEvents.on('job:created', () => healthMonitor.recordJobCreated());
  jobEvents.on('job:completed', () => healthMonitor.recordJobCompleted());
  jobEvents.on('job:failed', () => healthMonitor.recordJobFailed());
  jobEvents.on('job:dead', () => healthMonitor.recordJobDead());

  // Start periodic health logging
  healthMonitor.startPeriodicLogging();

  // Fork API server workers (one per CPU)
  const apiWorkers = [];
  for (let i = 0; i < NUM_CPUS; i++) {
    const worker = cluster.fork({ WORKER_ID: i });
    apiWorkers.push(worker);
    
    logger.info({
      event: 'master:worker-forked',
      workerId: worker.id,
      pid: worker.process.pid
    });
  }

  // Handle worker exits
  cluster.on('exit', (worker, code, signal) => {
    logger.error({
      event: 'master:worker-exit',
      workerId: worker.id,
      pid: worker.process.pid,
      code,
      signal
    });

    // Restart worker if not shutting down
    if (!gracefulShutdown.isShutdown()) {
      logger.info({ event: 'master:restarting-worker' });
      const newWorker = cluster.fork();
      const index = apiWorkers.indexOf(worker);
      if (index > -1) {
        apiWorkers[index] = newWorker;
      }
    }
  });

  // Register graceful shutdown handlers
  gracefulShutdown.registerHandler(async () => {
    logger.info({ event: 'shutdown:stopping-scheduler' });
    scheduler.pause();
    await scheduler.waitForActiveExecutions();
    scheduler.stop();
  }, 'scheduler');

  gracefulShutdown.registerHandler(async () => {
    logger.info({ event: 'shutdown:stopping-worker-pool' });
    await workerPool.shutdown();
  }, 'worker-pool');

  gracefulShutdown.registerHandler(async () => {
    logger.info({ event: 'shutdown:stopping-api-workers' });
    
    // Send shutdown signal to all API workers
    for (const worker of apiWorkers) {
      if (worker.isConnected()) {
        worker.send({ type: 'shutdown' });
      }
    }

    // Wait for workers to exit
    await new Promise((resolve) => {
      let exitedWorkers = 0;
      cluster.on('exit', () => {
        exitedWorkers++;
        if (exitedWorkers === apiWorkers.length) {
          resolve();
        }
      });

      // Force kill after timeout
      setTimeout(() => {
        apiWorkers.forEach(worker => {
          if (!worker.isDead()) {
            worker.kill();
          }
        });
        resolve();
      }, 10000);
    });
  }, 'api-cluster');

  // Setup signal handlers
  gracefulShutdown.setupSignalHandlers();

  logger.info({
    event: 'master:started',
    pid: process.pid,
    workers: NUM_CPUS
  });
}

/**
 * Worker Process (API Server)
 * Handles HTTP requests
 * Never executes jobs
 */
async function startWorker() {
  logger.info({
    event: 'worker:starting',
    pid: process.pid,
    workerId: process.env.WORKER_ID
  });

  // Setup global error handlers
  setupGlobalErrorHandlers();

  // Create and start server
  const app = createServer();
  const server = await startServer(app, PORT);

  // Handle shutdown message from master
  process.on('message', async (message) => {
    if (message.type === 'shutdown') {
      logger.info({ event: 'worker:shutdown-received' });
      
      try {
        await stopServer(server);
        process.exit(0);
      } catch (error) {
        logger.error({
          event: 'worker:shutdown-error',
          error: error.message
        });
        process.exit(1);
      }
    }
  });

  // Handle direct signals (if worker receives them)
  process.on('SIGTERM', async () => {
    await stopServer(server);
    process.exit(0);
  });

  process.on('SIGINT', async () => {
    await stopServer(server);
    process.exit(0);
  });

  logger.info({
    event: 'worker:started',
    pid: process.pid,
    port: PORT
  });
}

/**
 * Main Entry Point
 * Determines if this is master or worker process
 */
async function main() {
  if (cluster.isMaster || cluster.isPrimary) {
    await startMaster();
  } else {
    await startWorker();
  }
}

// Start the application
main().catch((error) => {
  logger.error({
    event: 'main:error',
    error: error.message,
    stack: error.stack
  });
  process.exit(1);
});