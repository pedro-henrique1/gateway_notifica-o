import amqp from 'amqplib';
import { config } from '../config.js';

export interface NotificationJob {
  notificationId: string;
}

type RabbitConnection = Awaited<ReturnType<typeof amqp.connect>>;
type RabbitChannel = Awaited<ReturnType<RabbitConnection['createChannel']>>;

let connection: RabbitConnection | null = null;
let channel: RabbitChannel | null = null;

export async function connectRabbitMQ(): Promise<RabbitChannel> {
  if (channel) {
    return channel;
  }

  connection = await amqp.connect(config.RABBITMQ_URL);
  connection.on('close', () => {
    connection = null;
    channel = null;
  });
  connection.on('error', (error) => {
    console.error('RabbitMQ connection error:', error);
  });

  channel = await connection.createChannel();
  await channel.assertQueue(config.RABBITMQ_MAIN_QUEUE, {
    durable: true,
  });

  return channel;
}

export async function publishNotificationJob(job: NotificationJob): Promise<void> {
  const activeChannel = await connectRabbitMQ();
  const content = Buffer.from(JSON.stringify(job));

  const isPublished = activeChannel.sendToQueue(config.RABBITMQ_MAIN_QUEUE, content, {
    persistent: true,
    contentType: 'application/json',
  });

  if (!isPublished) {
    throw new Error('Failed to publish notification job to RabbitMQ');
  }
}

export async function closeRabbitMQ(): Promise<void> {
  await channel?.close();
  await connection?.close();
  channel = null;
  connection = null;
}
