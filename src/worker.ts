import { config } from './config.js';
import { initDatabase, getNotificationById, updateNotificationStatus } from './database/prisma.js';
import { connectRabbitMQ, type NotificationJob } from './queue/rabbitmq.js';
import { FakeNotificationProvider } from './provider/fakeNotificationEmailProvider.js';

const fakeProvider = new FakeNotificationProvider();

async function handleNotificationJob(job: NotificationJob): Promise<void> {
  const notification = await getNotificationById(job.notificationId);

  if (!notification) {
    throw new Error(`Notification ${job.notificationId} was not found`);
  }

  await fakeProvider.send(notification);
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
      await handleNotificationJob(job);
      channel.ack(message);
    } catch (error) {
      console.error('Worker failed to process notification:', error);

      try {
        const job = JSON.parse(message.content.toString('utf8')) as NotificationJob;
        await updateNotificationStatus(job.notificationId, 'FAILED');
      } catch (updateError) {
        console.error('Failed to update notification status:', updateError);
      }

      channel.ack(message);
    }
  }, {
    noAck: false,
  });

  console.log('Worker is waiting for notifications');
}

void bootstrap();
