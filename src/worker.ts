import { config } from './config/config.js';
import { initDatabase, getNotificationById, updateNotificationStatus } from './database/prisma.js';
import { logger } from './config/logger.js';
import {
  connectRabbitMQ,
  publishToQueue,
  type NotificationJob,
} from './queue/rabbitmq.js';
import { FailingNotificationEmailProvider } from './provider/failingNotificationEmailProvider.js';
import { FakeNotificationProvider } from './provider/fakeNotificationEmailProvider.js';

const randomFailProvider = new FakeNotificationProvider();
const failingProvider = new FailingNotificationEmailProvider();
const MAX_RETRY_ATTEMPTS = 3;
const MAX_DELIVERY_ATTEMPTS = MAX_RETRY_ATTEMPTS + 1;

function getNotificationProvider() {
  return config.NOTIFICATION_PROVIDER_MODE === 'always-fail' ? failingProvider : randomFailProvider;
}

function getRetryAttempt(message: { properties: { headers?: Record<string, unknown> } }): number {
  const attempt = message.properties.headers?.['x-retry-attempt'];

  if (typeof attempt === 'number' && Number.isInteger(attempt) && attempt >= 1) {
    return attempt;
  }

  if (typeof attempt === 'string') {
    const parsedAttempt = Number(attempt);

    if (Number.isInteger(parsedAttempt) && parsedAttempt >= 1) {
      return parsedAttempt;
    }
  }

  return 1;
}

async function moveToRetryQueue(job: NotificationJob, attempt: number): Promise<void> {
  const nextAttempt = attempt + 1;

  if (nextAttempt === 2) {
    await publishToQueue(config.RABBITMQ_RETRY_1M_QUEUE, job, nextAttempt);
    return;
  }

  if (nextAttempt === 3) {
    await publishToQueue(config.RABBITMQ_RETRY_5M_QUEUE, job, nextAttempt);
    return;
  }

  if (nextAttempt === 4) {
    await publishToQueue(config.RABBITMQ_RETRY_15M_QUEUE, job, nextAttempt);
    return;
  }

  throw new Error(`Invalid retry attempt: ${nextAttempt}`);
}

async function moveToDlq(job: NotificationJob, attempt: number): Promise<void> {
  await publishToQueue(config.RABBITMQ_DLQ_QUEUE, job, attempt);
  await updateNotificationStatus(job.notificationId, 'FAILED');
}

async function handleNotificationJob(job: NotificationJob): Promise<void> {
  const notification = await getNotificationById(job.notificationId);

  if (!notification) {
    throw new Error(`Notification ${job.notificationId} was not found`);
  }

  await getNotificationProvider().send(notification);
  await updateNotificationStatus(notification.id, 'SENT');
}

async function bootstrap(): Promise<void> {
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

      jobLogger.info('processing notification');
      await handleNotificationJob(job);
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

void bootstrap();
