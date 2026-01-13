const { parentPort, workerData } = require('worker_threads');
const TaskRunner = require('./taskRunner');

/**
 * Worker Thread
 * Executes jobs in isolation from main thread
 * Communicates via message passing
 * Never crashes the main process
 */

// Initialize task runner for this worker
const taskRunner = new TaskRunner();

/**
 * Handle incoming messages from parent
 * Expected message format: { type: 'execute', job: {...} }
 */
parentPort.on('message', async (message) => {
  if (message.type === 'execute') {
    await executeJob(message.job);
  } else if (message.type === 'terminate') {
    await cleanup();
  }
});

/**
 * Execute a job and report result back to parent
 * @param {Object} job - Job to execute
 */
async function executeJob(job) {
  try {
    // Execute the job
    const result = await taskRunner.executeJob(job);

    // Send success message back to parent
    parentPort.postMessage({
      type: 'success',
      jobId: job.id,
      result
    });
  } catch (error) {
    // Send failure message back to parent
    // Never throw - always communicate via message
    parentPort.postMessage({
      type: 'failure',
      jobId: job.id,
      error: {
        message: error.message,
        stack: error.stack,
        name: error.name
      }
    });
  }
}

/**
 * Cleanup before worker termination
 */
async function cleanup() {
  try {
    await taskRunner.cleanup();
    parentPort.postMessage({ type: 'terminated' });
  } catch (error) {
    parentPort.postMessage({ 
      type: 'terminated', 
      error: error.message 
    });
  }
}

/**
 * Handle uncaught errors in worker
 * Report to parent instead of crashing
 */
process.on('uncaughtException', (error) => {
  parentPort.postMessage({
    type: 'worker-error',
    error: {
      message: error.message,
      stack: error.stack,
      name: error.name
    }
  });
});

process.on('unhandledRejection', (reason, promise) => {
  parentPort.postMessage({
    type: 'worker-error',
    error: {
      message: reason?.message || String(reason),
      stack: reason?.stack,
      name: 'UnhandledRejection'
    }
  });
});

// Signal that worker is ready
parentPort.postMessage({ type: 'ready' });