import { config } from '../config/config.js';
import { initDatabase } from '../database/prisma.js';
import { logger } from '../config/logger.js';
import { connectRabbitMQ, type NotificationJob } from '../queue/rabbitmq.js';
import { deliverNotification } from './notificationDelivery.js';
import { MAX_DELIVERY_ATTEMPTS, getRetryAttempt, moveToDlq, moveToRetryQueue } from './retryPolicy.js';

export async function bootstrapNotificationConsumer(): Promise<void> {
  await initDatabase();

  const channel = await connectRabbitMQ();
  await channel.prefetch(config.WORKER_PREFETCH);

  await channel.consume(config.RABBITMQ_MAIN_QUEUE, async (message) => {
    if (!message) {
      return;
    }

    try {
      const job = JSON.parse(message.content.toString('utf8')) as NotificationJob;
      const attempt = getRetryAttempt(message);
      const jobLogger = logger.child({ notificationId: job.notificationId, attempt });

      jobLogger.info({ queue: config.RABBITMQ_MAIN_QUEUE }, 'notification message received');
      await deliverNotification(job);
      jobLogger.info('notification sent');
      channel.ack(message);
    } catch (error) {
      const job = JSON.parse(message.content.toString('utf8')) as NotificationJob;
      const attempt = getRetryAttempt(message);
      const jobLogger = logger.child({ notificationId: job.notificationId, attempt });

      jobLogger.error({ err: error }, 'worker failed to process notification');

      try {
        if (attempt >= MAX_DELIVERY_ATTEMPTS) {
          await moveToDlq(job, attempt);
          jobLogger.warn('notification moved to dlq');
        } else {
          await moveToRetryQueue(job, attempt);
          jobLogger.info({ nextAttempt: attempt + 1 }, 'notification requeued for retry');
        }

        channel.ack(message);
      } catch (updateError) {
        jobLogger.error({ err: updateError }, 'failed to move notification to retry or dlq');
      }
    }
  }, {
    noAck: false,
  });

  logger.info('worker is waiting for notifications');
}