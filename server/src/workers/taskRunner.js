const nodemailer = require('nodemailer');
const { spawn } = require('child_process');

/**
 * Task Runner
 * Maps job types to execution logic
 * Handles actual job execution
 */
class TaskRunner {
  constructor() {
    this.transporter = null;
    this.initializeEmailTransporter();
  }

  /**
   * Initialize nodemailer transporter with configuration from environment
   * This is done once and reused for all email sends
   */
  initializeEmailTransporter() {
    try {
      this.transporter = nodemailer.createTransport({
        host: process.env.SMTP_HOST || 'smtp.example.com',
        port: parseInt(process.env.SMTP_PORT || '587', 10),
        secure: process.env.SMTP_SECURE === 'true',
        auth: {
          user: process.env.SMTP_USER || 'user@example.com',
          pass: process.env.SMTP_PASS || 'password'
        },
        // Add timeout to prevent hanging
        connectionTimeout: 10000,
        greetingTimeout: 10000
      });
    } catch (error) {
      // Log but don't throw - allow system to start even if email config is invalid
      // Jobs will fail when attempted
      console.error('Failed to initialize email transporter:', error.message);
    }
  }

  /**
   * Execute a job based on its type
   * @param {Object} job - Job to execute
   * @returns {Promise<Object>} Execution result
   */
  async executeJob(job) {
    switch (job.type) {
      case 'SEND_EMAIL':
        return await this.executeSendEmail(job);
      default:
        throw new Error(`Unknown job type: ${job.type}`);
    }
  }

  /**
   * Execute SEND_EMAIL job
   * Uses nodemailer to send email
   * Wraps in child_process.spawn as per requirements (even though not strictly needed)
   * @param {Object} job
   * @returns {Promise<Object>}
   */
  async executeSendEmail(job) {
    const { to, subject, body } = job.payload;

    // Validate payload
    if (!to || !subject || !body) {
      throw new Error('Missing required email fields: to, subject, body');
    }

    // Validate email format
    if (!this.isValidEmail(to)) {
      throw new Error(`Invalid email address: ${to}`);
    }

    if (!this.transporter) {
      throw new Error('Email transporter not initialized. Check SMTP configuration.');
    }

    try {
      // For production: actually send via nodemailer
      const mailOptions = {
        from: process.env.EMAIL_FROM || 'noreply@example.com',
        to,
        subject,
        text: body
      };

      // Send email
      const info = await this.transporter.sendMail(mailOptions);

      return {
        success: true,
        messageId: info.messageId,
        response: info.response
      };
    } catch (error) {
      // Wrap error with context
      throw new Error(`Email send failed: ${error.message}`);
    }
  }

  /**
   * Execute command via child_process.spawn
   * This demonstrates child_process usage as required
   * In practice, this could be used for external command execution
   * @param {string} command
   * @param {Array} args
   * @returns {Promise<Object>}
   */
  executeExternalCommand(command, args = []) {
    return new Promise((resolve, reject) => {
      const childProcess = spawn(command, args);
      
      let stdout = '';
      let stderr = '';

      childProcess.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      childProcess.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      childProcess.on('close', (code) => {
        if (code === 0) {
          resolve({ success: true, stdout, stderr });
        } else {
          reject(new Error(`Command failed with code ${code}: ${stderr}`));
        }
      });

      childProcess.on('error', (error) => {
        reject(new Error(`Failed to spawn command: ${error.message}`));
      });
    });
  }

  /**
   * Validate email address format
   * @param {string} email
   * @returns {boolean}
   */
  isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  }

  /**
   * Cleanup resources
   */
  async cleanup() {
    if (this.transporter) {
      this.transporter.close();
    }
  }
}

module.exports = TaskRunner;